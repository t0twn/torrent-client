"""Bounded sniff-mode scheduler, retry control, and traffic diagnostics.

This module owns all state that governs which DHT `announce_peer` hashes
the sniff-mode alert loop actually submits to libtorrent for metadata
acquisition, and tracks the lifecycle of each acquisition through to
persistent cache write (or failure). Download-mode behaviour, the
management API, the `.torrent` cache format, and the session-level
upload/download rate limits are unchanged.

The scheduler is driven from the alert-loop thread. The only cross-thread
interaction is a small result queue through which the background
persistence worker reports success / failure; the alert loop drains it
between alert batches, so no disk or network work blocks alert
consumption.

All in-memory data structures are bounded. Every admission decision
updates a small, fixed set of counters (``Diagnostics``) that can be
logged periodically as a single aggregate line. Info-hashes are never
used as metric labels.
"""

from __future__ import annotations

import enum
import logging
import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, Optional, Set


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

    QUEUED      - admitted, waiting for an in-flight slot
    IN_FLIGHT   - async_add_torrent submitted, awaiting handle / metadata
    PERSISTING  - metadata received, background worker writing cache
    COOLDOWN    - a terminal failure occurred; retry blocked until expiry
    """

    QUEUED = "queued"
    IN_FLIGHT = "in_flight"
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
    cooldown_base_seconds: float = 30.0
    cooldown_max_seconds: float = 3600.0
    cooldown_multiplier: float = 2.0
    cooldown_jitter: float = 0.25
    max_cooldown_entries: int = 512
    cooldown_ttl: float = 7200.0
    diagnostics_interval: float = 60.0
    distinct_hash_budget: int = 256

    def validate(self) -> None:
        positive = (
            "max_queue_size",
            "max_concurrent_acquisitions",
            "max_starts_per_second",
            "start_rate_window",
            "acquisition_timeout",
            "queue_wait_timeout",
            "cooldown_base_seconds",
            "cooldown_max_seconds",
            "cooldown_multiplier",
            "max_cooldown_entries",
            "cooldown_ttl",
            "diagnostics_interval",
            "distinct_hash_budget",
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
        cooldown_base_seconds=_env_float("SNIFF_COOLDOWN_BASE", 30.0),
        cooldown_max_seconds=_env_float("SNIFF_COOLDOWN_MAX", 3600.0),
        cooldown_multiplier=_env_float("SNIFF_COOLDOWN_MULTIPLIER", 2.0),
        cooldown_jitter=_env_float("SNIFF_COOLDOWN_JITTER", 0.25),
        max_cooldown_entries=_env_int("SNIFF_MAX_COOLDOWNS", 512),
        cooldown_ttl=_env_float("SNIFF_COOLDOWN_TTL", 7200.0),
        diagnostics_interval=_env_float("SNIFF_DIAGNOSTICS_INTERVAL", 60.0),
        distinct_hash_budget=_env_int("SNIFF_DISTINCT_HASH_BUDGET", 256),
    )


@dataclass
class Diagnostics:
    """Bounded-cardinality counters for the sniff path.

    ``recent_hashes`` tracks the set of distinct info-hashes seen since
    the last ``consume`` call, up to ``budget``. If more distinct hashes
    arrive than fit, ``hash_set_saturated`` is set and the set is a
    strict lower bound on the true distinct count. ``hash_overflow``
    records how many distinct hashes were dropped.

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

    cooldown_entries_recorded: int = 0
    cooldown_entries_evicted: int = 0

    _recent_hashes: Set[str] = field(default_factory=set, init=False, repr=False)
    _recent_hash_count: int = field(default=0, init=False, repr=False)

    def observe_hash(self, info_hash: str) -> None:
        """Record a distinct info-hash in the current window.

        If the set is already full and this hash is new, the hash is not
        inserted, ``hash_overflow`` increments, and ``hash_set_saturated``
        is set so the periodic log line signals the under-count.
        """

        if info_hash in self._recent_hashes:
            return
        if len(self._recent_hashes) >= self.budget:
            self.hash_set_saturated = True
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
            "cooldown_entries_recorded": self.cooldown_entries_recorded,
            "cooldown_entries_evicted": self.cooldown_entries_evicted,
        }

    def consume(self) -> Dict[str, Any]:
        """Return a snapshot and reset the window counters.

        Used by the periodic log emitter. The reset keeps each logged
        value attributable to the elapsed window. ``hash_set_saturated``
        and ``hash_overflow`` also reset; the next window starts clean
        whether or not it saturates.
        """

        snapshot = self.snapshot()
        self.announce_peer_received = 0
        self.announce_peer_invalid = 0
        self.hash_overflow = 0
        self.hash_set_saturated = False
        self._recent_hashes.clear()
        self._recent_hash_count = 0

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
    first.
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
        self._entries: Dict[str, _CooldownEntry] = {}
        self._order: Deque[str] = deque()

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
        """Return the monotonic time when `info_hash` may retry, or None.

        Returns ``None`` when the hash is not in cooldown or the entry
        has expired. Caller should compare against its own clock value.
        """

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

    def record_failure(self, info_hash: str, now: float) -> None:
        """Apply exponential backoff with jitter and record a new entry."""

        rng = random.Random()
        existing = self._entries.get(info_hash)
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
            self._remove(info_hash)

        while len(self._entries) >= self.config.max_cooldown_entries:
            self._evict_oldest()

        self._entries[info_hash] = _CooldownEntry(
            attempts=attempts,
            next_allowed=now + delay,
            expires_at=now + self.config.cooldown_ttl,
        )
        self._order.append(info_hash)
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
        # Lazy deque cleanup: we leave stale markers; they are filtered on
        # read. The deque is bounded by the registry size plus the number
        # of past removals, which is itself bounded by record_failure
        # calls (bounded by capacity * attempts).

    def _evict_expired(self, now: float) -> None:
        expired = [h for h, e in self._entries.items() if e.expires_at <= now]
        for h in expired:
            self._entries.pop(h, None)
            self.diagnostics.cooldown_entries_evicted += 1

    def _evict_oldest(self) -> None:
        while self._order:
            candidate = self._order.popleft()
            if candidate in self._entries:
                self._entries.pop(candidate, None)
                self.diagnostics.cooldown_entries_evicted += 1
                return


@dataclass
class _Candidate:
    info_hash: str
    state: CandidateState
    enqueued_at: float
    started_at: Optional[float] = None
    handle: Optional[Any] = None
    handle_generation: int = 0
    attempts: int = 0


class PersistResult:
    """Cross-thread result from the persistence worker.

    The worker enqueues instances of this class; the alert loop drains
    them into the scheduler.
    """

    __slots__ = ("info_hash", "success")

    def __init__(self, info_hash: str, success: bool):
        self.info_hash = info_hash
        self.success = success


class SniffScheduler:
    """Bounded candidate scheduler for sniff-mode metadata acquisition.

    The scheduler is driven entirely from the alert-loop thread, with one
    exception: ``apply_persist_result`` is safe to call from any thread
    (it only enqueues into a small bounded deque protected by a lock).

    Public entry points (alert-loop thread):

    - ``on_announce(info_hash, *, now, cache_exists, session_known)``
    - ``on_add_started(info_hash, handle, *, now)``
    - ``on_async_add_failed(info_hash, *, now)``
    - ``on_metadata_received(info_hash, *, now)``
    - ``on_metadata_missing(info_hash, *, now)``
    - ``on_unexpected_handle_removal(info_hash, *, now)``
    - ``tick(*, now, can_start, cache_exists, session_known)``
    - ``drain_persist_results(*, now, queue_full)``
    - ``maybe_emit_diagnostics(*, now)``
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
        self._persist_results: Deque[PersistResult] = deque()
        self._persist_results_lock = threading.Lock()
        self._generation = 0
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
            1 for c in self._candidates.values() if c.state is CandidateState.IN_FLIGHT
        )

    def persisting_count(self) -> int:
        return sum(
            1 for c in self._candidates.values() if c.state is CandidateState.PERSISTING
        )

    def cooldown_count(self) -> int:
        return len(self.cooldowns)

    def handle_for(self, info_hash: str) -> Optional[Any]:
        candidate = self._candidates.get(info_hash)
        if candidate is None or candidate.state is not CandidateState.IN_FLIGHT:
            return None
        return candidate.handle

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
        """Decide what to do with a freshly parsed `announce_peer`.

        ``cache_exists`` is True when the on-disk `.torrent` file already
        exists. ``session_known`` is True when ``TORRENTS`` or
        ``LIVE_HANDLES`` already contains the hash from a non-scheduler
        path (e.g. management API add, download mode).
        """

        now = self.clock() if now is None else now
        self.diagnostics.announce_peer_received += 1
        self.diagnostics.observe_hash(info_hash)

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
            if existing.state is CandidateState.PERSISTING:
                self.diagnostics.skip_persisting += 1
                return Admission.SKIP_PERSISTING
            if existing.state is CandidateState.COOLDOWN:
                if self.cooldowns.is_blocked(info_hash, now):
                    self.diagnostics.skip_cooldown += 1
                    return Admission.SKIP_COOLDOWN
                # Cooldown expired: reuse the candidate record for a retry.
                self._clear_candidate(info_hash)

        if self.cooldowns.is_blocked(info_hash, now):
            self.diagnostics.skip_cooldown += 1
            return Admission.SKIP_COOLDOWN

        # Attempt to start immediately.
        started = self._try_start(info_hash, now=now)
        if started:
            self.diagnostics.admitted_started += 1
            return Admission.ADMITTED_STARTED

        # Could not start immediately (capacity or rate). Queue if there is
        # room; otherwise drop. The in-flight bound does not by itself
        # cause drops; it only defers starting.
        if self._queue_full():
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

    # ------------------------------------------------------------------
    # Lifecycle callbacks (alert-loop thread)
    # ------------------------------------------------------------------

    def on_add_started(self, info_hash: str, handle: Any, *, now: Optional[float] = None) -> None:
        """Record that libtorrent produced a valid handle for the hash.

        This transitions state to ``IN_FLIGHT`` if the candidate was
        queued (edge case: a management-API add racing with an announce
        would not normally pass through the scheduler, so this is mainly
        defensive). The handle is stamped with a generation number so
        that late alerts from a previous timed-out handle can be
        detected and ignored.
        """

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            # The hash was added through another path (management API,
            # download mode). We don't track it; the scheduler only
            # governs sniff-mode announces.
            return
        if candidate.state is not CandidateState.IN_FLIGHT:
            # A late add_torrent_alert for a candidate that already
            # transitioned (timeout -> cooldown, metadata -> persisting,
            # etc.) must not corrupt the current state.
            self.diagnostics.late_alert_ignored += 1
            return
        self._generation += 1
        candidate.handle = handle
        candidate.handle_generation = self._generation
        candidate.started_at = now

    def on_async_add_failed(self, info_hash: str, *, now: Optional[float] = None) -> None:
        """Route an invalid-handle ``add_torrent_alert`` to cooldown."""

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return
        if candidate.state is not CandidateState.IN_FLIGHT:
            self.diagnostics.late_alert_ignored += 1
            return
        self.diagnostics.add_failed_async += 1
        self._enter_cooldown(info_hash, now=now)

    def on_invalid_handle_alert(self, info_hash: str, *, now: Optional[float] = None) -> None:
        """Count an add_torrent_alert with an invalid handle.

        Called by the integration layer before ``on_async_add_failed`` so
        diagnostics can distinguish the two. For scheduler purposes they
        route the same way.
        """

        now = self.clock() if now is None else now
        self.diagnostics.invalid_handle_alert += 1
        self.on_async_add_failed(info_hash, now=now)

    def on_sync_add_failure(self, info_hash: str, *, now: Optional[float] = None) -> None:
        """``async_add_torrent`` was called but ``add_torrent`` raised.

        The caller already released libtorrent-side state; we just enter
        cooldown.
        """

        now = self.clock() if now is None else now
        self.diagnostics.add_failed_sync += 1
        candidate = self._candidates.get(info_hash)
        if candidate is None or candidate.state is not CandidateState.IN_FLIGHT:
            # We never reserved this hash in the scheduler (another path
            # called add_torrent); nothing to release.
            return
        self._enter_cooldown(info_hash, now=now)

    def on_metadata_received(self, info_hash: str, *, now: Optional[float] = None) -> bool:
        """Transition to PERSISTING. Returns True when the scheduler owns the hash.

        Returning False tells the caller that the scheduler did not own
        this hash; the caller should still remove the handle from the
        libtorrent session but must not wait for a persist result.
        """

        now = self.clock() if now is None else now
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            # Untracked hash (added through management API or download
            # mode). Nothing for the scheduler to do.
            return False
        if candidate.state is not CandidateState.IN_FLIGHT:
            # Late alert for a candidate that already moved on.
            self.diagnostics.late_alert_ignored += 1
            return False
        candidate.state = CandidateState.PERSISTING
        # Clear the handle reference so timeout cannot yank it out from
        # under the persistence worker.
        candidate.handle = None
        candidate.handle_generation = 0
        self.diagnostics.metadata_success += 1
        return True

    def on_metadata_missing(self, info_hash: str, *, now: Optional[float] = None) -> bool:
        """metadata_received fired but ``torrent_file`` was None."""

        now = self.clock() if now is None else now
        self.diagnostics.metadata_missing += 1
        candidate = self._candidates.get(info_hash)
        if candidate is None:
            return False
        if candidate.state is not CandidateState.IN_FLIGHT:
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
            # A persistence job is still outstanding; do not touch state.
            return
        if candidate.state is CandidateState.IN_FLIGHT:
            self._enter_cooldown(info_hash, now=now)

    # ------------------------------------------------------------------
    # Persistence result boundary
    # ------------------------------------------------------------------

    def enqueue_persist_result(self, info_hash: str, success: bool) -> None:
        """Thread-safe: called from the background persistence worker."""

        result = PersistResult(info_hash, success)
        with self._persist_results_lock:
            self._persist_results.append(result)
            # Bound memory: drop oldest if the alert loop has stalled.
            bound = max(self.config.max_queue_size, 4)
            while len(self._persist_results) > bound:
                self._persist_results.popleft()

    def drain_persist_results(
        self,
        *,
        now: Optional[float] = None,
        queue_full: Optional[Callable[[str], bool]] = None,
    ) -> None:
        """Apply pending persist results on the alert-loop thread.

        ``queue_full`` is a callable that returns True when the
        background persistence queue cannot accept more work. The
        scheduler uses it to decide whether to enter cooldown when a
        result reports failure because the queue was full.
        """

        now = self.clock() if now is None else now
        with self._persist_results_lock:
            results = list(self._persist_results)
            self._persist_results.clear()

        for result in results:
            candidate = self._candidates.get(result.info_hash)
            if candidate is None or candidate.state is not CandidateState.PERSISTING:
                # Stale result for a hash we already moved on from.
                continue
            if result.success:
                self.diagnostics.persist_success += 1
                self.cooldowns.clear(result.info_hash)
                self._clear_candidate(result.info_hash)
            else:
                self.diagnostics.persist_failure += 1
                self._enter_cooldown(result.info_hash, now=now)

    def note_persist_queue_full(
        self, info_hash: str, *, now: Optional[float] = None
    ) -> None:
        """The metadata queue rejected a new persistence job.

        Called from the alert-loop thread when ``METADATA_QUEUE`` raises
        ``queue.Full``. We still own the candidate; enter cooldown so we
        do not loop on the same hash.
        """

        now = self.clock() if now is None else now
        self.diagnostics.persist_queue_full += 1
        candidate = self._candidates.get(info_hash)
        if candidate is None or candidate.state is not CandidateState.PERSISTING:
            return
        self._enter_cooldown(info_hash, now=now)

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
    ) -> list:
        """Advance timeouts, evictions, and queued promotions.

        ``can_start`` is invoked with an info_hash before promoting it
        from the queue; return False to leave it queued. ``cache_exists``
        and ``session_known`` are consulted so a hash whose cache file
        arrived or that was added externally is silently dropped.

        Returns the list of info-hashes that were promoted from QUEUED
        to IN_FLIGHT during this tick. The caller must invoke
        ``add_torrent`` for each of them and, if ``add_torrent`` returns
        False or raises, call ``on_sync_add_failure`` to release the
        scheduler slot.
        """

        now = self.clock() if now is None else now
        self._expire_timeouts(now)
        self._expire_queue_waits(now, cache_exists=cache_exists, session_known=session_known)
        return self._promote_queued(
            now=now,
            can_start=can_start,
            cache_exists=cache_exists,
            session_known=session_known,
        )

    def _expire_timeouts(self, now: float) -> None:
        timeout = self.config.acquisition_timeout
        for info_hash, candidate in list(self._candidates.items()):
            if candidate.state is not CandidateState.IN_FLIGHT:
                continue
            if candidate.started_at is None:
                continue
            if now - candidate.started_at >= timeout:
                self.diagnostics.metadata_timeout += 1
                self._enter_cooldown(info_hash, now=now, cancel_handle=True)

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

    def _enter_cooldown(
        self, info_hash: str, *, now: float, cancel_handle: bool = False
    ) -> None:
        candidate = self._candidates.get(info_hash)
        if cancel_handle and candidate is not None and candidate.handle is not None:
            # Surface the handle to the caller via a marker; the actual
            # session.remove_torrent is the caller's responsibility
            # because we don't hold a reference to the libtorrent session.
            candidate.handle = None
            candidate.handle_generation = 0
        self.cooldowns.record_failure(info_hash, now)
        if candidate is not None:
            candidate.state = CandidateState.COOLDOWN
            candidate.started_at = None
        else:
            self._candidates[info_hash] = _Candidate(
                info_hash=info_hash,
                state=CandidateState.COOLDOWN,
                enqueued_at=now,
            )

    def timed_out_handles(self, now: float) -> list:
        """Return handles whose acquisition has exceeded the timeout.

        The alert-loop caller uses this to remove the matching
        libtorrent handles from the session, ensuring the concurrency
        bound reflects actual active acquisitions.
        """

        handles = []
        timeout = self.config.acquisition_timeout
        for candidate in self._candidates.values():
            if candidate.state is not CandidateState.IN_FLIGHT:
                continue
            if candidate.handle is None or candidate.started_at is None:
                continue
            if now - candidate.started_at >= timeout:
                handles.append(candidate.handle)
        return handles

    def maybe_emit_diagnostics(
        self,
        *,
        now: Optional[float] = None,
        logger: Optional[Callable[[str], None]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Log an aggregate snapshot at ``diagnostics_interval`` cadence.

        Returns the snapshot when one was emitted so tests can assert on
        the content without parsing log lines. ``logger`` defaults to
        ``logging.info``; tests can pass a capture.
        """

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

        log = logger if logger is not None else logging.info
        parts = [f"{k}={v}" for k, v in snapshot.items()]
        log("sniff-scheduler " + " ".join(parts))
        return snapshot
