"""Bounded sniff-mode scheduler, retry control, and traffic diagnostics.

This module owns all state that governs which DHT `announce_peer` hashes
the sniff-mode alert loop actually submits to libtorrent for metadata
acquisition, and tracks the lifecycle of each acquisition through to
persistent cache write (or failure). Download-mode behaviour, the
management API, the `.torrent` cache format, and the session-level
upload/download rate limits are unchanged.

The scheduler is driven from the alert-loop thread. The only cross-thread
interaction is a small result dict through which the background
persistence worker reports success / failure; the alert loop drains it
between alert batches, so no disk or network work blocks alert
consumption.

All in-memory data structures are bounded. Every admission decision
updates a small, fixed set of counters (``Diagnostics``) that can be
logged periodically as a single aggregate line. Info-hashes are never
used as metric labels or included in log messages.
"""

from __future__ import annotations

import enum
import logging
import os
import random
import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer") from exc


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be a number") from exc


class CandidateState(enum.Enum):
    """Per-hash scheduler state.

    QUEUED           - admitted, waiting for an in-flight slot
    IN_FLIGHT        - async_add_torrent submitted, awaiting handle / metadata
    PENDING_REMOVAL  - acquisition timed out; caller must remove libtorrent
                       handle; slot is NOT yet released
    PERSISTING       - metadata received, background worker writing cache
    COOLDOWN         - a terminal failure occurred; retry blocked until expiry
    """

    QUEUED = "queued"
    IN_FLIGHT = "in_flight"
    PENDING_REMOVAL = "pending_removal"
    PERSISTING = "persisting"
    COOLDOWN = "cooldown"


class Admission(enum.Enum):
    """Outcome of ``SniffScheduler.on_announce``.

    Callers use the value to decide whether to invoke
    ``session.async_add_torrent`` immediately, defer work, or drop the
    announce.
    """

    SKIP_CACHED = "skip_cached"
    SKIP_PENDING = "skip_pending"
    SKIP_IN_FLIGHT = "skip_in_flight"
    SKIP_PERSISTING = "skip_persisting"
    SKIP_COOLDOWN = "skip_cooldown"
    SKIP_KNOWN_TO_SESSION = "skip_known_to_session"
    DROP_QUEUE_FULL = "drop_queue_full"
    ADMITTED_QUEUED = "admitted_queued"
    ADMITTED_STARTED = "admitted_started"


@dataclass
class SniffSchedulerConfig:
    """Tunable scheduler bounds. All defaults are conservative.

    Each field is validated by ``validate``; non-positive values are
    rejected because they would either disable a bound or invert its
    meaning.
    """

    max_queue_size: int = 64
    max_concurrent_acquisitions: int = 4
    max_starts_per_second: float = 1.0
    start_rate_window: float = 1.0
    acquisition_timeout: float = 120.0
    queue_wait_timeout: float = 300.0
    persist_timeout: float = 600.0
    cooldown_base_seconds: float = 30.0
    cooldown_max_seconds: float = 3600.0
    cooldown_multiplier: float = 2.0
    cooldown_jitter: float = 0.25
    max_cooldown_entries: int = 512
    cooldown_ttl: float = 7200.0
    diagnostics_interval: float = 60.0
    distinct_hash_budget: int = 256
    max_candidates: int = 2048
    queue_drop_cooldown_seconds: float = 30.0

    def validate(self) -> None:
        positive = (
            "max_queue_size",
            "max_concurrent_acquisitions",
            "max_starts_per_second",
            "start_rate_window",
            "acquisition_timeout",
            "queue_wait_timeout",
            "persist_timeout",
            "cooldown_base_seconds",
            "cooldown_max_seconds",
            "cooldown_multiplier",
            "max_cooldown_entries",
            "cooldown_ttl",
            "diagnostics_interval",
            "distinct_hash_budget",
            "max_candidates",
            "queue_drop_cooldown_seconds",
        )
        for name in positive:
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} must be positive (got {value})")
        if not 0.0 <= self.cooldown_jitter <= 1.0:
            raise ValueError(
                f"cooldown_jitter must be between 0 and 1 (got {self.cooldown_jitter})"
            )
        if self.cooldown_max_seconds < self.cooldown_base_seconds:
            raise ValueError("cooldown_max_seconds must be >= cooldown_base_seconds")


def config_from_environment() -> SniffSchedulerConfig:
    """Build a config from ``SNIFF_*`` environment variables."""

    return SniffSchedulerConfig(
        max_queue_size=_env_int("SNIFF_MAX_QUEUE", 64),
        max_concurrent_acquisitions=_env_int("SNIFF_MAX_IN_FLIGHT", 4),
        max_starts_per_second=_env_float("SNIFF_MAX_STARTS_PER_SECOND", 1.0),
        start_rate_window=_env_float("SNIFF_START_RATE_WINDOW", 1.0),
        acquisition_timeout=_env_float("SNIFF_ACQUISITION_TIMEOUT", 120.0),
        queue_wait_timeout=_env_float("SNIFF_QUEUE_WAIT_TIMEOUT", 300.0),
        persist_timeout=_env_float("SNIFF_PERSIST_TIMEOUT", 600.0),
        cooldown_base_seconds=_env_float("SNIFF_COOLDOWN_BASE", 30.0),
        cooldown_max_seconds=_env_float("SNIFF_COOLDOWN_MAX", 3600.0),
        cooldown_multiplier=_env_float("SNIFF_COOLDOWN_MULTIPLIER", 2.0),
        cooldown_jitter=_env_float("SNIFF_COOLDOWN_JITTER", 0.25),
        max_cooldown_entries=_env_int("SNIFF_MAX_COOLDOWNS", 512),
        cooldown_ttl=_env_float("SNIFF_COOLDOWN_TTL", 7200.0),
        diagnostics_interval=_env_float("SNIFF_DIAGNOSTICS_INTERVAL", 60.0),
        distinct_hash_budget=_env_int("SNIFF_DISTINCT_HASH_BUDGET", 256),
        max_candidates=_env_int("SNIFF_MAX_CANDIDATES", 2048),
        queue_drop_cooldown_seconds=_env_float("SNIFF_QUEUE_DROP_COOLDOWN", 30.0),
    )


@dataclass
class Diagnostics:
    """Bounded-cardinality counters for the sniff path.

    ``recent_hashes`` tracks the set of distinct info-hashes seen since
    the last ``consume`` call, up to ``budget``. If more distinct hashes
    arrive than fit, ``hash_set_saturated`` is set. ``hash_overflow``
    counts how many *distinct* hashes arrived beyond the budget (not the
    number of extra announces of those hashes).

    All counters are monotonic non-negative integers; they never decrease
    within a reporting window except via ``consume``, which returns and
    resets a snapshot.
    """

    budget: int

    announce_peer_received: int = 0
    announce_peer_invalid: int = 0
    hash_overflow: int = 0
    hash_set_saturated: bool = False

    skip_cached_file: int = 0
    skip_pending: int = 0
    skip_in_flight: int = 0
    skip_persisting: int = 0
    skip_cooldown: int = 0
    skip_known_to_session: int = 0

    drop_queue_full: int = 0

    admitted_queued: int = 0
    admitted_started: int = 0

    add_started: int = 0
    add_failed_sync: int = 0
    add_failed_async: int = 0
    invalid_handle_alert: int = 0

    metadata_success: int = 0
    metadata_missing: int = 0
    metadata_timeout: int = 0
    late_alert_ignored: int = 0

    persist_success: int = 0
    persist_failure: int = 0
    persist_queue_full: int = 0

    queue_wait_timeouts: int = 0
    queue_evictions: int = 0
    candidate_evictions: int = 0

    cooldown_entries_recorded: int = 0
    cooldown_entries_evicted: int = 0

    _recent_hashes: Set[str] = field(default_factory=set, init=False, repr=False)
    _recent_hash_count: int = field(default=0, init=False, repr=False)
    # Bounded approximation for distinct overflow hashes. Once the budget
    # is exceeded we track up to ``overflow_sample_budget`` distinct
    # hashes; beyond that we only increment the counter. The counter is
    # then a lower bound on the true distinct-overflow count; the
    # ``hash_set_saturated`` flag signals this on the periodic log line.
    _overflow_hashes: Set[str] = field(default_factory=set, init=False, repr=False)
    _overflow_untracked: int = field(default=0, init=False, repr=False)

    # Memory bound for the overflow sample: a small multiple of the main
    # budget keeps the per-window overhead bounded even under a DHT burst.
    _overflow_sample_budget: int = 64

    def observe_hash(self, info_hash: str) -> None:
        """Record a distinct info-hash in the current window.

        If the recent-hash set is already full and this hash is new, the
        hash is not inserted into the recent set. If the overflow sample
        set still has room, the hash is remembered there so repeat
        announces of the same overflow hash do not inflate the counter.
        Once both sets are full, ``hash_overflow`` increments and
        ``hash_set_saturated`` is set; the counter is then a lower bound
        on the true distinct-overflow count.
        """

        if info_hash in self._recent_hashes:
            return
        if info_hash in self._overflow_hashes:
            return
        if len(self._recent_hashes) >= self.budget:
            self.hash_set_saturated = True
            if len(self._overflow_hashes) < self._overflow_sample_budget:
                self._overflow_hashes.add(info_hash)
                self.hash_overflow += 1
            else:
                self._overflow_untracked += 1
                self.hash_overflow += 1
            return
        self._recent_hashes.add(info_hash)
        self._recent_hash_count += 1

    def snapshot(self) -> Dict[str, Any]:
        """Return the current counters as a plain dict for logging."""

        return {
            "announce_peer_received": self.announce_peer_received,
            "announce_peer_invalid": self.announce_peer_invalid,
            "distinct_hashes": self._recent_hash_count,
            "hash_set_saturated": self.hash_set_saturated,
            "hash_overflow": self.hash_overflow,
            "skip_cached_file": self.skip_cached_file,
            "skip_pending": self.skip_pending,
            "skip_in_flight": self.skip_in_flight,
            "skip_persisting": self.skip_persisting,
            "skip_cooldown": self.skip_cooldown,
            "skip_known_to_session": self.skip_known_to_session,
            "drop_queue_full": self.drop_queue_full,
            "admitted_queued": self.admitted_queued,
            "admitted_started": self.admitted_started,
            "add_started": self.add_started,
            "add_failed_sync": self.add_failed_sync,
            "add_failed_async": self.add_failed_async,
            "invalid_handle_alert": self.invalid_handle_alert,
            "metadata_success": self.metadata_success,
            "metadata_missing": self.metadata_missing,
            "metadata_timeout": self.metadata_timeout,
            "late_alert_ignored": self.late_alert_ignored,
            "persist_success": self.persist_success,
            "persist_failure": self.persist_failure,
            "persist_queue_full": self.persist_queue_full,
            "queue_wait_timeouts": self.queue_wait_timeouts,
            "queue_evictions": self.queue_evictions,
            "candidate_evictions": self.candidate_evictions,
            "cooldown_entries_recorded": self.cooldown_entries_recorded,
            "cooldown_entries_evicted": self.cooldown_entries_evicted,
        }

    def consume(self) -> Dict[str, Any]:
        """Return a snapshot and reset the window counters."""

        snapshot = self.snapshot()
        self.announce_peer_received = 0
        self.announce_peer_invalid = 0
        self.hash_overflow = 0
        self.hash_set_saturated = False
        self._recent_hashes.clear()
        self._recent_hash_count = 0
        self._overflow_hashes.clear()
        self._overflow_untracked = 0

        self.skip_cached_file = 0
        self.skip_pending = 0
        self.skip_in_flight = 0
        self.skip_persisting = 0
        self.skip_cooldown = 0
        self.skip_known_to_session = 0

        self.drop_queue_full = 0

        self.admitted_queued = 0
        self.admitted_started = 0

        self.add_started = 0
        self.add_failed_sync = 0
        self.add_failed_async = 0
        self.invalid_handle_alert = 0

        self.metadata_success = 0
        self.metadata_missing = 0
        self.metadata_timeout = 0
        self.late_alert_ignored = 0

        self.persist_success = 0
        self.persist_failure = 0
        self.persist_queue_full = 0

        self.queue_wait_timeouts = 0
        self.queue_evictions = 0
        self.candidate_evictions = 0

        self.cooldown_entries_recorded = 0
        self.cooldown_entries_evicted = 0
        return snapshot


@dataclass
class _CooldownEntry:
    attempts: int
    next_allowed: float
    expires_at: float


class CooldownRegistry:
    """Bounded, expiring, per-hash backoff registry.

    The registry is NOT durable. Process restart clears it; this is
    intentional because the `.torrent` cache is the authoritative source
    for "already acquired successfully" and the cooldowns only exist to
    throttle retries of failed acquisitions.

    Entries are evicted when the registry reaches capacity (oldest
    insertion first) or when an entry's TTL elapses, whichever comes
    first. The registry also exposes ``evicted_hashes`` after
    ``record_failure`` / ``_evict_*`` so the scheduler can clean up
    matching candidate records and keep overall memory bounded.
    """

    def __init__(
        self,
        config: SniffSchedulerConfig,
        diagnostics: Diagnostics,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.config = config
        self.diagnostics = diagnostics
        self.clock = clock
        # OrderedDict gives O(1) removal at any position while preserving
        # insertion order for eviction. This prevents the unbounded deque
        # growth that a separate deque + set pair would produce across
        # repeated record_failure / clear cycles.
        self._entries: "OrderedDict[str, _CooldownEntry]" = OrderedDict()
        self.evicted_hashes: List[str] = []

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, info_hash: str) -> bool:
        entry = self._entries.get(info_hash)
        if entry is None:
            return False
        if entry.expires_at <= self.clock():
            self._remove(info_hash)
            return False
        return True

    def until_allowed(self, info_hash: str) -> Optional[float]:
        entry = self._entries.get(info_hash)
        if entry is None:
            return None
        if entry.expires_at <= self.clock():
            self._remove(info_hash)
            return None
        return entry.next_allowed

    def is_blocked(self, info_hash: str, now: float) -> bool:
        entry = self._entries.get(info_hash)
        if entry is None:
            return False
        if entry.expires_at <= now:
            self._remove(info_hash)
            return False
        return entry.next_allowed > now

    def record_failure(
        self,
        info_hash: str,
        now: float,
        *,
        override_delay: Optional[float] = None,
    ) -> None:
        """Apply exponential backoff with jitter and record a new entry.

        ``evicted_hashes`` is populated with hashes that were removed by
        expiry or capacity eviction during this call, so the scheduler
        can clean up their candidate records.

        ``override_delay`` uses a fixed delay instead of computing the
        backoff; used for bounded overload suppression (queue-drop
        cooldown) where the normal exponential curve would be too long.
        """

        self.evicted_hashes = []
        rng = random.Random()
        existing = self._entries.get(info_hash)

        if override_delay is not None:
            delay = max(0.0, float(override_delay))
            attempts = (existing.attempts + 1) if existing else 1
        else:
            attempts = (existing.attempts + 1) if existing else 1
            delay = min(
                self.config.cooldown_base_seconds
                * (self.config.cooldown_multiplier ** (attempts - 1)),
                self.config.cooldown_max_seconds,
            )
            jitter = delay * self.config.cooldown_jitter
            delay += rng.uniform(-jitter, jitter)
            delay = max(0.0, delay)

        self._evict_expired(now)

        if info_hash in self._entries:
            del self._entries[info_hash]

        while len(self._entries) >= self.config.max_cooldown_entries:
            self._evict_oldest()

        self._entries[info_hash] = _CooldownEntry(
            attempts=attempts,
            next_allowed=now + delay,
            expires_at=now + self.config.cooldown_ttl,
        )
        self.diagnostics.cooldown_entries_recorded += 1

    def clear(self, info_hash: str) -> None:
        """Remove any cooldown for `info_hash` (successful acquisition)."""

        self._remove(info_hash)

    def entries_snapshot(self) -> Dict[str, Dict[str, float]]:
        """Return a copy of active entries (test / diagnostics helper)."""

        now = self.clock()
        return {
            h: {
                "attempts": e.attempts,
                "next_allowed": e.next_allowed,
                "expires_at": e.expires_at,
            }
            for h, e in self._entries.items()
            if e.expires_at > now
        }

    def _remove(self, info_hash: str) -> None:
        self._entries.pop(info_hash, None)

    def _evict_expired(self, now: float) -> None:
        expired = [h for h, e in self._entries.items() if e.expires_at <= now]
        for h in expired:
            del self._entries[h]
            self.diagnostics.cooldown_entries_evicted += 1
            self.evicted_hashes.append(h)

    def _evict_oldest(self) -> None:
        if not self._entries:
            return
        # OrderedDict preserves insertion order; pop the first item.
        oldest_hash, _ = self._entries.popitem(last=False)
        self.diagnostics.cooldown_entries_evicted += 1
        self.evicted_hashes.append(oldest_hash)


@dataclass
class _Candidate:
    info_hash: str
    state: CandidateState
    enqueued_at: float
    started_at: Optional[float] = None
    persisting_at: Optional[float] = None
    pending_removal_at: Optional[float] = None
    handle: Optional[Any] = None
    handle_generation: int = 0
    attempts: int = 0
    # Unique per-PERSISTING-transition identifier so a stale result from
    # an earlier attempt cannot settle the current attempt.
    persisting_job_id: int = 0
    # Time at which async_add_torrent was last called for this attempt.
    # Used to disambiguate stale alerts that arrive after the new
    # attempt's handle has not yet been registered.
    add_torrent_called_at: Optional[float] = None


class PersistResult:
    """Cross-thread result from the persistence worker.

    Carries ``job_id`` so the alert-loop drain can match the result to
    the specific attempt it belongs to; a stale result from an earlier
    attempt that timed out cannot settle a newer attempt.
    """

    __slots__ = ("info_hash", "success", "job_id")

    def __init__(self, info_hash: str, success: bool, job_id: int = 0):
        self.info_hash = info_hash
        self.success = success
        self.job_id = job_id


class SniffScheduler:
    """Bounded candidate scheduler for sniff-mode metadata acquisition.

    The scheduler is driven entirely from the alert-loop thread, with one
    exception: ``enqueue_persist_result`` is safe to call from any thread
    (it writes into a bounded dict protected by a lock).

    Lifecycle overview:

    - ``on_announce`` admits a hash or skips/drops it.
    - ``on_add_started`` records the handle for a given attempt.
    - ``on_async_add_failed`` / ``on_invalid_handle_alert`` route a
      failed add to cooldown.
    - ``on_metadata_received`` / ``on_metadata_missing`` transition
      ``IN_FLIGHT`` to ``PERSISTING`` or cooldown.
    - ``on_persist_queue_full`` treats a full persistence queue as an
      ``IN_FLIGHT`` terminal failure (slot released, cooldown entered,
      ``TORRENTS`` reservation cleared by caller).
    - ``tick`` promotes queued candidates, expires queue waits, and
      returns timed-out handles for the caller to remove from the
      libtorrent session. The slot is released only after the caller
      invokes ``ack_handle_removed``.
    - ``drain_persist_results`` applies cross-thread results.

    Late alerts (from a previous timed-out handle) are validated against
    the candidate's stored handle reference so they cannot mutate a new
    attempt for the same hash.
    """

    def __init__(
        self,
        config: SniffSchedulerConfig,
        diagnostics: Optional[Diagnostics] = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        config.validate()
        self.config = config
        self.diagnostics = diagnostics or Diagnostics(budget=config.distinct_hash_budget)
        self.clock = clock
        self.cooldowns = CooldownRegistry(config, self.diagnostics, clock=clock)

        self._candidates: Dict[str, _Candidate] = {}
        self._queue: Deque[str] = deque()
        self._start_times: Deque[float] = deque()
        # Keyed by (info_hash, job_id): the worker may report a result
        # before a duplicate result for the same hash; only the latest
        # with the matching job_id matters.
        self._persist_results: Dict[Tuple[str, int], PersistResult] = {}
        self._persist_results_lock = threading.Lock()
        self._generation = 0
        self._persisting_job_counter = 0
        self._last_diagnostics_at: Optional[float] = None

    # ------------------------------------------------------------------
    # Public query helpers
    # ------------------------------------------------------------------

    def state_of(self, info_hash: str) -> Optional[CandidateState]:
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return None
        return candidate.state

    def queued_count(self) -> int:
        return sum(
            1 for c in self._candidates.values() if c.state is CandidateState.QUEUED
        )

    def in_flight_count(self) -> int:
        return sum(
            1 for c in self._candidates.values()
            if c.state in (CandidateState.IN_FLIGHT, CandidateState.PENDING_REMOVAL)
        )

    def persisting_count(self) -> int:
        return sum(
            1 for c in self._candidates.values() if c.state is CandidateState.PERSISTING
        )

    def cooldown_count(self) -> int:
        return len(self.cooldowns)

    def candidate_count(self) -> int:
        return len(self._candidates)

    def handle_for(self, info_hash: str) -> Optional[Any]:
        candidate = self._candidates.get(info_hash)
        if candidate is None or candidate.state is not CandidateState.IN_FLIGHT:
            return None
        return candidate.handle

    def generation_for(self, info_hash: str) -> int:
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return 0
        return candidate.handle_generation

    # ------------------------------------------------------------------
    # Announce admission
    # ------------------------------------------------------------------

    def on_announce(
        self,
        info_hash: str,
        *,
        now: Optional[float] = None,
        cache_exists: bool = False,
        session_known: bool = False,
    ) -> Admission:
        """Decide what to do with a freshly parsed `announce_peer`."""

        now = self.clock() if now is None else now
        self.diagnostics.announce_peer_received += 1
        self.diagnostics.observe_hash(info_hash)
        self._sweep_orphan_cooldowns()

        if cache_exists:
            self.diagnostics.skip_cached_file += 1
            return Admission.SKIP_CACHED

        if session_known:
            self.diagnostics.skip_known_to_session += 1
            return Admission.SKIP_KNOWN_TO_SESSION

        existing = self._candidates.get(info_hash)
        if existing is not None:
            if existing.state is CandidateState.QUEUED:
                self.diagnostics.skip_pending += 1
                return Admission.SKIP_PENDING
            if existing.state is CandidateState.IN_FLIGHT:
                self.diagnostics.skip_in_flight += 1
                return Admission.SKIP_IN_FLIGHT
            if existing.state is CandidateState.PENDING_REMOVAL:
                self.diagnostics.skip_in_flight += 1
                return Admission.SKIP_IN_FLIGHT
            if existing.state is CandidateState.PERSISTING:
                self.diagnostics.skip_persisting += 1
                return Admission.SKIP_PERSISTING
            if existing.state is CandidateState.COOLDOWN:
                if self.cooldowns.is_blocked(info_hash, now):
                    self.diagnostics.skip_cooldown += 1
                    return Admission.SKIP_COOLDOWN
                self._clear_candidate(info_hash)

        if self.cooldowns.is_blocked(info_hash, now):
            self.diagnostics.skip_cooldown += 1
            return Admission.SKIP_COOLDOWN

        started = self._try_start(info_hash, now=now)
        if started:
            self.diagnostics.admitted_started += 1
            return Admission.ADMITTED_STARTED

        if self._queue_full():
            self.diagnostics.drop_queue_full += 1
            # Apply a short cooldown so the same hash doesn't immediately
            # re-announce and hit the full queue again (bounded overload
            # suppression). The queue-drop cooldown is deliberately
            # shorter than acquisition-failure cooldown.
            self.cooldowns.record_failure(
                info_hash,
                now=now,
                override_delay=self.config.queue_drop_cooldown_seconds,
            )
            self._touch_candidate_cooldown(info_hash, now=now)
            return Admission.DROP_QUEUE_FULL

        if not self._reserve_candidate_slot(info_hash, now=now):
            self.diagnostics.drop_queue_full += 1
            return Admission.DROP_QUEUE_FULL

        self._enqueue(info_hash, now=now)
        self.diagnostics.admitted_queued += 1
        return Admission.ADMITTED_QUEUED

    # ------------------------------------------------------------------
    # Start helpers
    # ------------------------------------------------------------------

    def _queue_full(self) -> bool:
        return self.queued_count() >= self.config.max_queue_size

    def _reserve_candidate_slot(self, info_hash: str, now: float) -> bool:
        """Reserve memory for a new queued candidate.

        Bounded by ``max_candidates``; if we're at capacity, evict an
        orphan COOLDOWN entry to make room. Returns False only when no
        eviction candidate exists.
        """

        if len(self._candidates) < self.config.max_candidates:
            return True
        # Try to evict orphan COOLDOWN entries (no cooldown registry entry).
        for h, c in list(self._candidates.items()):
            if c.state is CandidateState.COOLDOWN and h not in self.cooldowns:
                self._candidates.pop(h, None)
                self.diagnostics.candidate_evictions += 1
                if len(self._candidates) < self.config.max_candidates:
                    return True
        return False

    def _rate_limited(self, now: float) -> bool:
        window = self.config.start_rate_window
        while self._start_times and self._start_times[0] <= now - window:
            self._start_times.popleft()
        return len(self._start_times) >= self.config.max_starts_per_second

    def _enqueue(self, info_hash: str, *, now: float) -> None:
        candidate = _Candidate(
            info_hash=info_hash,
            state=CandidateState.QUEUED,
            enqueued_at=now,
        )
        self._candidates[info_hash] = candidate
        self._queue.append(info_hash)

    def _try_start(self, info_hash: str, *, now: float) -> bool:
        if self.in_flight_count() >= self.config.max_concurrent_acquisitions:
            return False
        if self._rate_limited(now):
            return False

        candidate = self._candidates.get(info_hash)
        if candidate is None:
            if len(self._candidates) >= self.config.max_candidates:
                return False
            candidate = _Candidate(
                info_hash=info_hash,
                state=CandidateState.IN_FLIGHT,
                enqueued_at=now,
                started_at=now,
            )
            self._candidates[info_hash] = candidate
        else:
            candidate.state = CandidateState.IN_FLIGHT
            candidate.started_at = now
            candidate.handle = None
            candidate.handle_generation = 0
        candidate.attempts += 1
        self._start_times.append(now)
        self.diagnostics.add_started += 1
        return True

    def _clear_candidate(self, info_hash: str) -> None:
        self._candidates.pop(info_hash, None)

    def _touch_candidate_cooldown(self, info_hash: str, *, now: float) -> None:
        """Create or update a COOLDOWN candidate record for `info_hash`.

        Used when a drop (queue full) applies a short cooldown so the
        candidate record matches the registry state.
        """

        existing = self._candidates.get(info_hash)
        if existing is None:
            if len(self._candidates) >= self.config.max_candidates:
                return
            self._candidates[info_hash] = _Candidate(
                info_hash=info_hash,
                state=CandidateState.COOLDOWN,
                enqueued_at=now,
            )
        else:
            existing.state = CandidateState.COOLDOWN
            existing.started_at = None
            existing.handle = None
            existing.handle_generation = 0

    # ------------------------------------------------------------------
    # Lifecycle callbacks (alert-loop thread)
    # ------------------------------------------------------------------

    def on_add_started(
        self, info_hash: str, handle: Any, *, now: Optional[float] = None
    ) -> None:
        """Record that libtorrent produced a valid handle for the hash.

        Validates that the candidate is still in ``IN_FLIGHT``. A late
        add_torrent_alert for a candidate that has already transitioned
        (timeout -> pending removal, metadata -> persisting, etc.) is
        counted as ``late_alert_ignored`` and otherwise dropped.
        """

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return
        if candidate.state is not CandidateState.IN_FLIGHT:
            self.diagnostics.late_alert_ignored += 1
            return
        self._generation += 1
        candidate.handle = handle
        candidate.handle_generation = self._generation
        candidate.started_at = now

    def on_async_add_failed(
        self,
        info_hash: str,
        *,
        now: Optional[float] = None,
        expected_generation: int = 0,
    ) -> None:
        """Route an invalid-handle ``add_torrent_alert`` to cooldown."""

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return
        if candidate.state is not CandidateState.IN_FLIGHT:
            self.diagnostics.late_alert_ignored += 1
            return
        if expected_generation and candidate.handle_generation != expected_generation:
            self.diagnostics.late_alert_ignored += 1
            return
        self.diagnostics.add_failed_async += 1
        self._enter_cooldown(info_hash, now=now)

    def on_invalid_handle_alert(
        self,
        info_hash: str,
        alert_handle: Any = None,
        *,
        now: Optional[float] = None,
    ) -> None:
        """Count an add_torrent_alert with an invalid handle.

        If the scheduler's candidate already has a DIFFERENT valid handle
        recorded (from a newer attempt's successful add_torrent_alert),
        this alert is stale and is counted in ``late_alert_ignored``
        rather than cooling down the current attempt.
        """

        now = self.clock() if now is None else now
        self.diagnostics.invalid_handle_alert += 1
        candidate = self._candidates.get(info_hash)
        if (
            candidate is not None
            and candidate.handle is not None
            and alert_handle is not None
            and candidate.handle is not alert_handle
        ):
            self.diagnostics.late_alert_ignored += 1
            return
        self.on_async_add_failed(info_hash, now=now)

    def on_sync_add_failure(
        self, info_hash: str, *, now: Optional[float] = None
    ) -> None:
        """``async_add_torrent`` was called but ``add_torrent`` raised."""

        now = self.clock() if now is None else now
        self.diagnostics.add_failed_sync += 1
        candidate = self._candidates.get(info_hash)
        if candidate is None or candidate.state is not CandidateState.IN_FLIGHT:
            return
        self._enter_cooldown(info_hash, now=now)

    def on_add_torrent_called(
        self, info_hash: str, *, now: Optional[float] = None
    ) -> None:
        """Record that async_add_torrent was submitted for a candidate.

        Called from the add_torrent wrapper after async_add_torrent has
        been successfully submitted. The timestamp is used to
        disambiguate stale alerts that arrive for candidates whose
        handle has not yet been registered via ``on_add_started``.
        """

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None or candidate.state is not CandidateState.IN_FLIGHT:
            return
        candidate.add_torrent_called_at = now

    def on_metadata_received(
        self,
        info_hash: str,
        handle: Any = None,
        *,
        now: Optional[float] = None,
    ) -> Optional[int]:
        """Transition to PERSISTING.

        When ``handle`` is supplied, it must match the candidate's
        recorded handle (by identity); otherwise the alert is treated as
        stale and counted in ``late_alert_ignored``.

        Returns the new ``persisting_job_id`` on success, or ``None`` if
        the alert was rejected. The caller passes this job_id through the
        metadata queue and into the persistence worker so the result can
        be matched back to this specific attempt.
        """

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return None
        if candidate.state is not CandidateState.IN_FLIGHT:
            self.diagnostics.late_alert_ignored += 1
            return None
        if handle is not None and candidate.handle is not None and candidate.handle is not handle:
            # Old handle from a previous attempt that has already timed
            # out (and whose removal has been acked) must not transition
            # the new attempt to PERSISTING.
            self.diagnostics.late_alert_ignored += 1
            return None
        self._persisting_job_counter += 1
        candidate.state = CandidateState.PERSISTING
        candidate.persisting_at = now
        candidate.persisting_job_id = self._persisting_job_counter
        candidate.handle = None
        candidate.handle_generation = 0
        self.diagnostics.metadata_success += 1
        return candidate.persisting_job_id

    def on_metadata_missing(
        self,
        info_hash: str,
        handle: Any = None,
        *,
        now: Optional[float] = None,
    ) -> bool:
        """metadata_received fired but ``torrent_file`` was None.

        When ``handle`` is supplied, validates ownership against the
        candidate's recorded handle. A stale alert from a previous
        attempt is rejected and counted in ``late_alert_ignored``.
        """

        now = self.clock() if now is None else now
        self.diagnostics.metadata_missing += 1
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return False
        if candidate.state is not CandidateState.IN_FLIGHT:
            self.diagnostics.late_alert_ignored += 1
            return False
        if handle is not None and candidate.handle is not None and candidate.handle is not handle:
            self.diagnostics.late_alert_ignored += 1
            return False
        self._enter_cooldown(info_hash, now=now)
        return True

    def on_unexpected_handle_removal(
        self, info_hash: str, *, now: Optional[float] = None
    ) -> None:
        """Handle vanished outside the scheduler's own transitions."""

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return
        if candidate.state is CandidateState.PERSISTING:
            return
        if candidate.state is CandidateState.IN_FLIGHT:
            self._enter_cooldown(info_hash, now=now)

    def on_persist_queue_full(
        self, info_hash: str, *, now: Optional[float] = None
    ) -> bool:
        """The metadata queue rejected a new persistence job.

        Treats queue-full as an ``IN_FLIGHT`` terminal failure: slot
        released, cooldown entered, and ``True`` is returned so the
        caller can also release any ``TORRENTS`` reservation it made on
        the candidate's behalf. Also accepts the case where the
        candidate is already ``PERSISTING`` (a previous queueing attempt
        succeeded but we still want to register the overflow).
        """

        now = self.clock() if now is None else now
        self.diagnostics.persist_queue_full += 1
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return False
        if candidate.state not in (
            CandidateState.IN_FLIGHT,
            CandidateState.PERSISTING,
        ):
            return False
        self._enter_cooldown(info_hash, now=now)
        return True

    # ------------------------------------------------------------------
    # Persistence result boundary
    # ------------------------------------------------------------------

    def enqueue_persist_result(
        self, info_hash: str, success: bool, job_id: int = 0
    ) -> None:
        """Thread-safe: called from the background persistence worker.

        Results are keyed by (info_hash, job_id). A result with an old
        job_id is ignored when the candidate's current persisting_job_id
        differs, so a stale result from an earlier timed-out attempt
        cannot settle a newer attempt.
        """

        result = PersistResult(info_hash, success, job_id)
        with self._persist_results_lock:
            key = (info_hash, job_id)
            self._persist_results[key] = result

    def drain_persist_results(
        self,
        *,
        now: Optional[float] = None,
    ) -> None:
        """Apply pending persist results on the alert-loop thread.

        Each result is matched to the candidate's current
        ``persisting_job_id``. A stale result (from an earlier attempt
        that timed out and was reused for a newer attempt) is discarded
        so it cannot settle the new attempt.
        """

        now = self.clock() if now is None else now
        with self._persist_results_lock:
            results = list(self._persist_results.values())
            self._persist_results.clear()

        for result in results:
            candidate = self._candidates.get(result.info_hash)
            if candidate is None or candidate.state is not CandidateState.PERSISTING:
                continue
            if result.job_id and candidate.persisting_job_id != result.job_id:
                # Stale result from an earlier attempt.
                continue
            if result.success:
                self.diagnostics.persist_success += 1
                self.cooldowns.clear(result.info_hash)
                self._clear_candidate(result.info_hash)
            else:
                self.diagnostics.persist_failure += 1
                self._enter_cooldown(result.info_hash, now=now)

    # ------------------------------------------------------------------
    # Periodic maintenance
    # ------------------------------------------------------------------

    def tick(
        self,
        *,
        now: Optional[float] = None,
        can_start: Optional[Callable[[str], bool]] = None,
        cache_exists: Optional[Callable[[str], bool]] = None,
        session_known: Optional[Callable[[str], bool]] = None,
    ) -> Tuple[List[str], List[Tuple[str, Any]]]:
        """Advance timeouts, evictions, and queued promotions.

        Returns a pair ``(promoted, pending_removals)``:

        - ``promoted`` is the list of info-hashes that were moved from
          QUEUED to IN_FLIGHT during this tick. The caller must invoke
          ``add_torrent`` for each of them and, if it fails, call
          ``on_sync_add_failure``.
        - ``pending_removals`` is the list of ``(info_hash, handle)``
          pairs whose acquisition has exceeded the timeout. The caller
          must remove each handle from the libtorrent session (and from
          ``LIVE_HANDLES`` / ``TORRENTS``) and then invoke
          ``ack_handle_removed`` to release the scheduler slot.
        """

        now = self.clock() if now is None else now
        self._sweep_orphan_cooldowns()
        self._expire_queue_waits(
            now, cache_exists=cache_exists, session_known=session_known
        )
        # PERSISTING timeout: if the persistence worker has not reported
        # within ``persist_timeout``, treat the candidate as stuck and
        # enter cooldown. This is the reliable reconciliation path for
        # the (rare) case where a result is dropped or lost.
        self._expire_persisting(now)
        pending = self._collect_pending_removals(now)
        promoted = self._promote_queued(
            now=now,
            can_start=can_start,
            cache_exists=cache_exists,
            session_known=session_known,
        )
        return promoted, pending

    def pending_removals(self, now: Optional[float] = None) -> List[Tuple[str, Any]]:
        """Return timed-out candidates, transitioning IN_FLIGHT ones to
        ``PENDING_REMOVAL``.

        Includes candidates already in ``PENDING_REMOVAL`` whose removal
        has not yet been acknowledged. This lets the caller retry a
        failed removal on the next loop iteration.
        """

        now = self.clock() if now is None else now
        return self._collect_pending_removals(now)

    def _expire_persisting(self, now: float) -> None:
        """Recover PERSISTING candidates stuck past ``persist_timeout``."""

        timeout = self.config.persist_timeout
        for info_hash, candidate in list(self._candidates.items()):
            if candidate.state is not CandidateState.PERSISTING:
                continue
            if candidate.persisting_at is None:
                continue
            if now - candidate.persisting_at >= timeout:
                self.diagnostics.persist_failure += 1
                self._enter_cooldown(info_hash, now=now)

    def _collect_pending_removals(self, now: float) -> List[Tuple[str, Any]]:
        timeout = self.config.acquisition_timeout
        result: List[Tuple[str, Any]] = []
        for info_hash, candidate in list(self._candidates.items()):
            # Return previously-timed-out candidates whose removal hasn't
            # been acknowledged yet (retry path for F1).
            if candidate.state is CandidateState.PENDING_REMOVAL:
                if candidate.handle is not None:
                    result.append((info_hash, candidate.handle))
                continue
            if candidate.state is not CandidateState.IN_FLIGHT:
                continue
            if candidate.started_at is None:
                continue
            if now - candidate.started_at < timeout:
                continue
            # Timed out. Two cases:
            # 1. Has a handle: return for caller to remove from session
            #    (slot released via ack_handle_removed).
            # 2. No handle (async_add_torrent called but add_torrent_alert
            #    never arrived or was lost): release slot directly, enter
            #    cooldown. This prevents a lost alert from permanently
            #    occupying an in-flight slot.
            if candidate.handle is not None:
                candidate.state = CandidateState.PENDING_REMOVAL
                candidate.pending_removal_at = now
                result.append((info_hash, candidate.handle))
                self.diagnostics.metadata_timeout += 1
            else:
                self.diagnostics.metadata_timeout += 1
                self._enter_cooldown(info_hash, now=now)
        return result

    def ack_handle_removed(self, info_hash: str, *, now: Optional[float] = None) -> None:
        """Caller has removed the libtorrent handle for a timed-out hash.

        Transitions the candidate to COOLDOWN and releases the slot.
        """

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return
        if candidate.state is not CandidateState.PENDING_REMOVAL:
            return
        self._enter_cooldown(info_hash, now=now, skip_record=True)

    def _expire_queue_waits(
        self,
        now: float,
        cache_exists: Optional[Callable[[str], bool]],
        session_known: Optional[Callable[[str], bool]],
    ) -> None:
        timeout = self.config.queue_wait_timeout
        survivors: Deque[str] = deque()
        while self._queue:
            info_hash = self._queue.popleft()
            candidate = self._candidates.get(info_hash)
            if candidate is None or candidate.state is not CandidateState.QUEUED:
                continue
            if cache_exists is not None and cache_exists(info_hash):
                self.diagnostics.skip_cached_file += 1
                self._clear_candidate(info_hash)
                continue
            if session_known is not None and session_known(info_hash):
                self.diagnostics.skip_known_to_session += 1
                self._clear_candidate(info_hash)
                continue
            if now - candidate.enqueued_at >= timeout:
                self.diagnostics.queue_wait_timeouts += 1
                self._enter_cooldown(info_hash, now=now)
                continue
            survivors.append(info_hash)
        self._queue = survivors

    def _promote_queued(
        self,
        now: float,
        can_start: Optional[Callable[[str], bool]],
        cache_exists: Optional[Callable[[str], bool]],
        session_known: Optional[Callable[[str], bool]],
    ) -> list:
        promoted: list = []
        while self._queue:
            if self.in_flight_count() >= self.config.max_concurrent_acquisitions:
                break
            if self._rate_limited(now):
                break
            info_hash = self._queue[0]
            candidate = self._candidates.get(info_hash)
            if candidate is None or candidate.state is not CandidateState.QUEUED:
                self._queue.popleft()
                continue
            if cache_exists is not None and cache_exists(info_hash):
                self.diagnostics.skip_cached_file += 1
                self._queue.popleft()
                self._clear_candidate(info_hash)
                continue
            if session_known is not None and session_known(info_hash):
                self.diagnostics.skip_known_to_session += 1
                self._queue.popleft()
                self._clear_candidate(info_hash)
                continue
            if can_start is not None and not can_start(info_hash):
                break
            self._queue.popleft()
            self._try_start(info_hash, now=now)
            promoted.append(info_hash)
        return promoted

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _sweep_orphan_cooldowns(self) -> None:
        """Remove COOLDOWN candidates whose registry entry is gone.

        Called at the start of on_announce and tick so memory stays
        bounded even when the cooldown registry evicts entries.
        """

        if not self._candidates:
            return
        for h in list(self._candidates):
            c = self._candidates[h]
            if c.state is CandidateState.COOLDOWN and h not in self.cooldowns:
                self._candidates.pop(h, None)
                self.diagnostics.candidate_evictions += 1

    def _enter_cooldown(
        self,
        info_hash: str,
        *,
        now: float,
        cancel_handle: bool = False,
        skip_record: bool = False,
    ) -> None:
        candidate = self._candidates.get(info_hash)
        if cancel_handle and candidate is not None and candidate.handle is not None:
            candidate.handle = None
            candidate.handle_generation = 0
        if not skip_record:
            self.cooldowns.record_failure(info_hash, now)
        if candidate is not None:
            candidate.state = CandidateState.COOLDOWN
            candidate.started_at = None
            candidate.handle = None
            candidate.handle_generation = 0
        else:
            if len(self._candidates) < self.config.max_candidates:
                self._candidates[info_hash] = _Candidate(
                    info_hash=info_hash,
                    state=CandidateState.COOLDOWN,
                    enqueued_at=now,
                )

    def maybe_emit_diagnostics(
        self,
        *,
        now: Optional[float] = None,
        logger: Optional[Callable[[str], None]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Log an aggregate snapshot at ``diagnostics_interval`` cadence."""

        now = self.clock() if now is None else now
        if self._last_diagnostics_at is None:
            self._last_diagnostics_at = now
            return None
        if now - self._last_diagnostics_at < self.config.diagnostics_interval:
            return None
        self._last_diagnostics_at = now
        snapshot = self.diagnostics.consume()
        snapshot["queued"] = self.queued_count()
        snapshot["in_flight"] = self.in_flight_count()
        snapshot["persisting"] = self.persisting_count()
        snapshot["cooldowns"] = self.cooldown_count()
        snapshot["candidates"] = self.candidate_count()

        log = logger if logger is not None else logging.info
        parts = [f"{k}={v}" for k, v in snapshot.items()]
        log("sniff-scheduler " + " ".join(parts))
        return snapshot
