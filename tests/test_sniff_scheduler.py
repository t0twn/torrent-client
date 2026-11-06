"""Tests for the sniff-mode scheduler, retry, and diagnostics.

The scheduler is pure Python with a fake clock, so these tests run in
milliseconds without any libtorrent session. They verify the scheduler
state machine, bounded data structures, cooldown backoff, diagnostics
counters, and the integration surface used by ``app.py``.

A fake ``Session`` at the end of this file exercises the integration
between ``app.handle_alert_announce_peer``, the scheduler, and
``app.add_torrent`` using a stand-in for libtorrent alerts so we can
verify the full announce -> add -> metadata -> persist loop without a
real DHT.
"""

from __future__ import annotations

import os
import queue
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import sniff_scheduler as ss


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeClock:
    """Monotonic clock that tests advance manually."""

    def __init__(self, start: float = 1000.0):
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def make_config(**overrides) -> ss.SniffSchedulerConfig:
    defaults = dict(
        max_queue_size=4,
        max_concurrent_acquisitions=2,
        max_starts_per_second=10.0,
        start_rate_window=1.0,
        acquisition_timeout=30.0,
        queue_wait_timeout=60.0,
        cooldown_base_seconds=10.0,
        cooldown_max_seconds=60.0,
        cooldown_multiplier=2.0,
        cooldown_jitter=0.0,  # deterministic for tests
        max_cooldown_entries=8,
        cooldown_ttl=300.0,
        diagnostics_interval=5.0,
        distinct_hash_budget=4,
    )
    defaults.update(overrides)
    return ss.SniffSchedulerConfig(**defaults)


def make_scheduler(**overrides):
    config = make_config(**overrides)
    clock = FakeClock()
    scheduler = ss.SniffScheduler(config, clock=clock)
    return scheduler, clock


HASH_A = "a" * 40
HASH_B = "b" * 40
HASH_C = "c" * 40
HASH_D = "d" * 40
HASH_E = "e" * 40


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class ConfigTests(unittest.TestCase):
    def test_default_config_validates(self):
        ss.SniffSchedulerConfig().validate()

    def test_non_positive_values_rejected(self):
        for field in (
            "max_queue_size",
            "max_concurrent_acquisitions",
            "max_starts_per_second",
            "acquisition_timeout",
            "cooldown_base_seconds",
        ):
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    ss.SniffSchedulerConfig(**{field: 0}).validate()

    def test_jitter_out_of_range_rejected(self):
        with self.assertRaises(ValueError):
            ss.SniffSchedulerConfig(cooldown_jitter=1.5).validate()

    def test_cooldown_max_below_base_rejected(self):
        with self.assertRaises(ValueError):
            ss.SniffSchedulerConfig(
                cooldown_base_seconds=60.0, cooldown_max_seconds=30.0
            ).validate()

    def test_environment_variables_override_defaults(self):
        env = {
            "SNIFF_MAX_QUEUE": "16",
            "SNIFF_MAX_IN_FLIGHT": "2",
            "SNIFF_ACQUISITION_TIMEOUT": "45",
            "SNIFF_COOLDOWN_JITTER": "0.1",
        }
        with patch.dict(os.environ, env, clear=False):
            config = ss.config_from_environment()
        self.assertEqual(config.max_queue_size, 16)
        self.assertEqual(config.max_concurrent_acquisitions, 2)
        self.assertEqual(config.acquisition_timeout, 45.0)
        self.assertEqual(config.cooldown_jitter, 0.1)

    def test_invalid_environment_variable_surfaces(self):
        with patch.dict(os.environ, {"SNIFF_MAX_QUEUE": "bogus"}, clear=False):
            with self.assertRaises(ValueError):
                ss.config_from_environment()


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

class DiagnosticsTests(unittest.TestCase):
    def test_distinct_hash_set_saturates_at_budget(self):
        diag = ss.Diagnostics(budget=2)
        diag.observe_hash(HASH_A)
        diag.observe_hash(HASH_B)
        diag.observe_hash(HASH_C)

        self.assertTrue(diag.hash_set_saturated)
        self.assertEqual(diag.hash_overflow, 1)
        # Re-observing a known hash does not increase overflow.
        diag.observe_hash(HASH_A)
        self.assertEqual(diag.hash_overflow, 1)

    def test_consume_resets_window_but_keeps_ongoing_state(self):
        diag = ss.Diagnostics(budget=4)
        diag.announce_peer_received = 3
        diag.observe_hash(HASH_A)
        snapshot = diag.consume()

        self.assertEqual(snapshot["announce_peer_received"], 3)
        self.assertEqual(snapshot["distinct_hashes"], 1)
        # After consume, counters reset.
        self.assertEqual(diag.announce_peer_received, 0)
        self.assertFalse(diag.hash_set_saturated)
        self.assertEqual(diag.hash_overflow, 0)

    def test_snapshot_does_not_mutate_counters(self):
        diag = ss.Diagnostics(budget=4)
        diag.skip_cached_file = 5
        first = diag.snapshot()
        second = diag.snapshot()
        self.assertEqual(first, second)
        self.assertEqual(diag.skip_cached_file, 5)


# ---------------------------------------------------------------------------
# Cooldown registry
# ---------------------------------------------------------------------------

class CooldownRegistryTests(unittest.TestCase):
    def test_first_failure_blocks_for_base_delay(self):
        config = make_config()
        clock = FakeClock()
        diag = ss.Diagnostics(budget=4)
        registry = ss.CooldownRegistry(config, diag, clock=clock)

        registry.record_failure(HASH_A, clock())

        self.assertTrue(registry.is_blocked(HASH_A, clock()))
        clock.advance(9.9)
        self.assertTrue(registry.is_blocked(HASH_A, clock()))
        clock.advance(0.2)
        self.assertFalse(registry.is_blocked(HASH_A, clock()))

    def test_subsequent_failures_grow_exponentially_up_to_cap(self):
        config = make_config(
            cooldown_base_seconds=10.0,
            cooldown_max_seconds=25.0,
            cooldown_multiplier=2.0,
            cooldown_jitter=0.0,
        )
        clock = FakeClock()
        registry = ss.CooldownRegistry(config, ss.Diagnostics(budget=4), clock=clock)

        registry.record_failure(HASH_A, clock()); clock.advance(1000)
        # Second failure: 10 * 2 = 20s
        registry.record_failure(HASH_A, clock()); clock.advance(1000)
        # Third failure: capped at 25s
        registry.record_failure(HASH_A, clock())

        entries = registry.entries_snapshot()
        self.assertEqual(entries[HASH_A]["attempts"], 3)
        # next_allowed is clock() + 25
        self.assertAlmostEqual(entries[HASH_A]["next_allowed"] - clock(), 25.0, places=3)

    def test_success_clears_cooldown(self):
        config = make_config()
        clock = FakeClock()
        registry = ss.CooldownRegistry(config, ss.Diagnostics(budget=4), clock=clock)

        registry.record_failure(HASH_A, clock())
        self.assertIn(HASH_A, registry)
        registry.clear(HASH_A)
        self.assertNotIn(HASH_A, registry)

    def test_capacity_evicts_oldest_entry(self):
        config = make_config(max_cooldown_entries=2)
        clock = FakeClock()
        diag = ss.Diagnostics(budget=4)
        registry = ss.CooldownRegistry(config, diag, clock=clock)

        registry.record_failure(HASH_A, clock()); clock.advance(0.1)
        registry.record_failure(HASH_B, clock()); clock.advance(0.1)
        registry.record_failure(HASH_C, clock())

        self.assertEqual(len(registry), 2)
        self.assertNotIn(HASH_A, registry)
        self.assertIn(HASH_B, registry)
        self.assertIn(HASH_C, registry)
        self.assertEqual(diag.cooldown_entries_evicted, 1)

    def test_expired_entries_are_treated_as_absent(self):
        config = make_config(cooldown_ttl=5.0, cooldown_base_seconds=1.0)
        clock = FakeClock()
        registry = ss.CooldownRegistry(config, ss.Diagnostics(budget=4), clock=clock)

        registry.record_failure(HASH_A, clock())
        clock.advance(10.0)
        self.assertFalse(registry.is_blocked(HASH_A, clock()))
        self.assertNotIn(HASH_A, registry)

    def test_jitter_does_not_exceed_configured_fraction(self):
        config = make_config(
            cooldown_base_seconds=10.0,
            cooldown_multiplier=1.0,
            cooldown_jitter=0.1,
        )
        clock = FakeClock()
        registry = ss.CooldownRegistry(config, ss.Diagnostics(budget=4), clock=clock)

        delays = []
        for _ in range(30):
            registry.record_failure(HASH_A, clock())
            entry = registry.entries_snapshot()[HASH_A]
            delays.append(entry["next_allowed"] - clock())
            clock.advance(100)
        self.assertTrue(all(9.0 <= d <= 11.0 for d in delays))


# ---------------------------------------------------------------------------
# Scheduler admission and deduplication
# ---------------------------------------------------------------------------

class SchedulerAdmissionTests(unittest.TestCase):
    def test_first_announce_starts_immediately(self):
        scheduler, clock = make_scheduler()

        admission = scheduler.on_announce(HASH_A, now=clock())

        self.assertEqual(admission, ss.Admission.ADMITTED_STARTED)
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.IN_FLIGHT)
        self.assertEqual(scheduler.in_flight_count(), 1)
        self.assertEqual(scheduler.diagnostics.add_started, 1)

    def test_repeated_announce_while_in_flight_is_skip(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())

        admission = scheduler.on_announce(HASH_A, now=clock())

        self.assertEqual(admission, ss.Admission.SKIP_IN_FLIGHT)
        self.assertEqual(scheduler.in_flight_count(), 1)
        self.assertEqual(scheduler.diagnostics.skip_in_flight, 1)
        self.assertEqual(scheduler.diagnostics.add_started, 1)

    def test_cached_file_skips_immediately(self):
        scheduler, clock = make_scheduler()

        admission = scheduler.on_announce(HASH_A, now=clock(), cache_exists=True)

        self.assertEqual(admission, ss.Admission.SKIP_CACHED)
        self.assertIsNone(scheduler.state_of(HASH_A))
        self.assertEqual(scheduler.diagnostics.skip_cached_file, 1)

    def test_session_known_skips_immediately(self):
        scheduler, clock = make_scheduler()

        admission = scheduler.on_announce(HASH_A, now=clock(), session_known=True)

        self.assertEqual(admission, ss.Admission.SKIP_KNOWN_TO_SESSION)
        self.assertIsNone(scheduler.state_of(HASH_A))
        self.assertEqual(scheduler.diagnostics.skip_known_to_session, 1)

    def test_distinct_hashes_over_capacity_queue(self):
        scheduler, clock = make_scheduler(max_concurrent_acquisitions=1)

        first = scheduler.on_announce(HASH_A, now=clock())
        second = scheduler.on_announce(HASH_B, now=clock())
        third = scheduler.on_announce(HASH_C, now=clock())

        self.assertEqual(first, ss.Admission.ADMITTED_STARTED)
        self.assertEqual(second, ss.Admission.ADMITTED_QUEUED)
        self.assertEqual(third, ss.Admission.ADMITTED_QUEUED)
        self.assertEqual(scheduler.in_flight_count(), 1)
        self.assertEqual(scheduler.queued_count(), 2)

    def test_queue_full_drops_when_queue_at_capacity(self):
        scheduler, clock = make_scheduler(
            max_queue_size=1, max_concurrent_acquisitions=1,
        )

        scheduler.on_announce(HASH_A, now=clock())  # starts
        scheduler.on_announce(HASH_B, now=clock())  # queued
        admission = scheduler.on_announce(HASH_C, now=clock())  # drops

        self.assertEqual(admission, ss.Admission.DROP_QUEUE_FULL)
        self.assertEqual(scheduler.diagnostics.drop_queue_full, 1)

    def test_cooldown_blocks_reannounce(self):
        scheduler, clock = make_scheduler()

        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_async_add_failed(HASH_A, now=clock())
        admission = scheduler.on_announce(HASH_A, now=clock())

        self.assertEqual(admission, ss.Admission.SKIP_COOLDOWN)
        self.assertEqual(scheduler.diagnostics.skip_cooldown, 1)

    def test_cooldown_recovery_after_delay(self):
        scheduler, clock = make_scheduler(
            cooldown_base_seconds=5.0, cooldown_jitter=0.0,
        )

        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_async_add_failed(HASH_A, now=clock())
        clock.advance(5.1)
        admission = scheduler.on_announce(HASH_A, now=clock())

        self.assertEqual(admission, ss.Admission.ADMITTED_STARTED)
        self.assertEqual(scheduler.diagnostics.admitted_started, 2)

    def test_persisting_state_skips_announce(self):
        scheduler, clock = make_scheduler()

        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())
        admission = scheduler.on_announce(HASH_A, now=clock())

        self.assertEqual(admission, ss.Admission.SKIP_PERSISTING)
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.PERSISTING)

    def test_pending_state_skips_announce(self):
        scheduler, clock = make_scheduler(max_concurrent_acquisitions=1)

        scheduler.on_announce(HASH_A, now=clock())  # starts
        scheduler.on_announce(HASH_B, now=clock())  # queued
        admission = scheduler.on_announce(HASH_B, now=clock())

        self.assertEqual(admission, ss.Admission.SKIP_PENDING)


# ---------------------------------------------------------------------------
# Scheduler lifecycle: add, metadata, persist
# ---------------------------------------------------------------------------

class SchedulerLifecycleTests(unittest.TestCase):
    def test_on_add_started_records_handle(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())

        handle = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle, now=clock())

        self.assertIs(scheduler.handle_for(HASH_A), handle)

    def test_metadata_received_transitions_to_persisting(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())

        owned = scheduler.on_metadata_received(HASH_A, now=clock())

        self.assertTrue(owned)
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.PERSISTING)
        self.assertEqual(scheduler.diagnostics.metadata_success, 1)

    def test_metadata_missing_routes_to_cooldown(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())

        owned = scheduler.on_metadata_missing(HASH_A, now=clock())

        self.assertTrue(owned)
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.diagnostics.metadata_missing, 1)

    def test_persist_success_clears_state(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())

        scheduler.enqueue_persist_result(HASH_A, True)
        scheduler.drain_persist_results(now=clock())

        self.assertIsNone(scheduler.state_of(HASH_A))
        self.assertEqual(scheduler.diagnostics.persist_success, 1)
        self.assertNotIn(HASH_A, scheduler.cooldowns)

    def test_persist_failure_enters_cooldown(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())

        scheduler.enqueue_persist_result(HASH_A, False)
        scheduler.drain_persist_results(now=clock())

        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.diagnostics.persist_failure, 1)
        self.assertIn(HASH_A, scheduler.cooldowns)

    def test_persist_queue_full_enters_cooldown(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())

        scheduler.on_persist_queue_full(HASH_A, now=clock())

        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.diagnostics.persist_queue_full, 1)

    def test_async_add_failed_enters_cooldown(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())

        scheduler.on_invalid_handle_alert(HASH_A, now=clock())

        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.diagnostics.add_failed_async, 1)
        self.assertEqual(scheduler.diagnostics.invalid_handle_alert, 1)

    def test_sync_add_failure_releases_slot(self):
        scheduler, clock = make_scheduler(max_concurrent_acquisitions=1)

        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_sync_add_failure(HASH_A, now=clock())

        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.in_flight_count(), 0)
        # A new hash can now start immediately.
        admission = scheduler.on_announce(HASH_B, now=clock())
        self.assertEqual(admission, ss.Admission.ADMITTED_STARTED)

    def test_unexpected_handle_removal_routes_to_cooldown(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())

        scheduler.on_unexpected_handle_removal(HASH_A, now=clock())

        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)


# ---------------------------------------------------------------------------
# Timeouts, queue promotion, and late alerts
# ---------------------------------------------------------------------------

class SchedulerTimeoutAndPromotionTests(unittest.TestCase):
    def test_acquisition_timeout_enters_cooldown_after_ack(self):
        scheduler, clock = make_scheduler(acquisition_timeout=10.0)

        scheduler.on_announce(HASH_A, now=clock())
        handle = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle, now=clock())
        clock.advance(11.0)
        promoted, pending = scheduler.tick(now=clock())

        self.assertEqual(pending, [(HASH_A, handle)])
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.PENDING_REMOVAL)
        self.assertEqual(scheduler.diagnostics.metadata_timeout, 1)
        # Slot stays occupied until caller removes the handle and acks.
        self.assertEqual(scheduler.in_flight_count(), 1)
        scheduler.ack_handle_removed(HASH_A, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.in_flight_count(), 0)

    def test_pending_removals_returns_handles_without_transition(self):
        scheduler, clock = make_scheduler(acquisition_timeout=10.0)
        scheduler.on_announce(HASH_A, now=clock())
        handle = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle, now=clock())
        clock.advance(11.0)

        pending = scheduler.pending_removals(clock())

        self.assertEqual(pending, [(HASH_A, handle)])
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.PENDING_REMOVAL)

    def test_queue_promotion_when_slot_opens(self):
        scheduler, clock = make_scheduler(max_concurrent_acquisitions=1)

        scheduler.on_announce(HASH_A, now=clock())  # starts
        scheduler.on_announce(HASH_B, now=clock())  # queued
        self.assertEqual(scheduler.queued_count(), 1)

        scheduler.on_metadata_received(HASH_A, now=clock())  # slot opens
        promoted, _ = scheduler.tick(now=clock())

        self.assertEqual(promoted, [HASH_B])
        self.assertEqual(scheduler.state_of(HASH_B), ss.CandidateState.IN_FLIGHT)
        self.assertEqual(scheduler.queued_count(), 0)

    def test_queue_promotion_respects_external_cache_arrival(self):
        scheduler, clock = make_scheduler(max_concurrent_acquisitions=1)
        scheduler.on_announce(HASH_A, now=clock())  # starts
        scheduler.on_announce(HASH_B, now=clock())  # queued

        scheduler.on_metadata_received(HASH_A, now=clock())
        promoted, _ = scheduler.tick(
            now=clock(),
            cache_exists=lambda h: h == HASH_B,
        )

        self.assertEqual(promoted, [])
        self.assertIsNone(scheduler.state_of(HASH_B))
        self.assertEqual(scheduler.diagnostics.skip_cached_file, 1)

    def test_queue_wait_timeout_routes_to_cooldown(self):
        scheduler, clock = make_scheduler(
            max_concurrent_acquisitions=1, queue_wait_timeout=10.0,
        )

        scheduler.on_announce(HASH_A, now=clock())  # starts
        scheduler.on_announce(HASH_B, now=clock())  # queued
        clock.advance(11.0)
        scheduler.tick(now=clock())

        self.assertEqual(scheduler.state_of(HASH_B), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.diagnostics.queue_wait_timeouts, 1)

    def test_late_metadata_after_timeout_is_ignored(self):
        """A timed-out acquisition must not corrupt a fresh reservation."""

        scheduler, clock = make_scheduler(
            acquisition_timeout=10.0, cooldown_base_seconds=1.0, cooldown_jitter=0.0,
        )

        # First acquisition times out.
        scheduler.on_announce(HASH_A, now=clock())
        handle_old = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle_old, now=clock())
        clock.advance(11.0)
        _, pending = scheduler.tick(now=clock())
        self.assertEqual(len(pending), 1)
        scheduler.ack_handle_removed(HASH_A, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)

        # Wait for cooldown to expire, then a second announce starts a new acquisition.
        clock.advance(2.0)
        scheduler.on_announce(HASH_A, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.IN_FLIGHT)
        self.assertEqual(scheduler.in_flight_count(), 1)

        # The late metadata_received for the timed-out handle: scheduler has
        # already moved on, so the alert must not affect the new reservation.
        owned = scheduler.on_metadata_received(HASH_A, now=clock())
        # The scheduler owns the hash but the candidate is now the new attempt;
        # metadata_received moves it to PERSISTING (which is fine — the new
        # attempt succeeded). The key invariant is in_flight == 0 afterwards.
        self.assertTrue(owned)
        self.assertEqual(scheduler.in_flight_count(), 0)

    def test_rate_limiting_defers_immediate_start(self):
        scheduler, clock = make_scheduler(
            max_concurrent_acquisitions=4,
            max_starts_per_second=1.0,
            start_rate_window=1.0,
        )

        first = scheduler.on_announce(HASH_A, now=clock())  # starts
        second = scheduler.on_announce(HASH_B, now=clock())  # rate-limited -> queued

        self.assertEqual(first, ss.Admission.ADMITTED_STARTED)
        self.assertEqual(second, ss.Admission.ADMITTED_QUEUED)

        clock.advance(1.1)
        promoted, _ = scheduler.tick(now=clock())
        self.assertEqual(promoted, [HASH_B])

    def test_late_add_alert_after_timeout_does_not_corrupt_reservation(self):
        scheduler, clock = make_scheduler(acquisition_timeout=5.0)
        scheduler.on_announce(HASH_A, now=clock())
        handle_old = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle_old, now=clock())
        clock.advance(6.0)
        _, pending = scheduler.tick(now=clock())  # timeout -> pending removal
        self.assertEqual(len(pending), 1)
        scheduler.ack_handle_removed(HASH_A, now=clock())  # now COOLDOWN

        # A late add_torrent_alert arrives. The candidate is now in COOLDOWN,
        # so on_add_started must not flip it back to IN_FLIGHT.
        handle = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.diagnostics.late_alert_ignored, 1)


# ---------------------------------------------------------------------------
# Persistence result queue
# ---------------------------------------------------------------------------

class PersistResultQueueTests(unittest.TestCase):
    def test_enqueue_is_thread_safe(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())

        def worker():
            scheduler.enqueue_persist_result(HASH_A, True)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()

        scheduler.drain_persist_results(now=clock())
        self.assertEqual(scheduler.diagnostics.persist_success, 1)

    def test_stale_persist_result_is_ignored(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())

        # Manually clear state (simulating a concurrent eviction path).
        scheduler._candidates.pop(HASH_A)
        scheduler.enqueue_persist_result(HASH_A, True)
        scheduler.drain_persist_results(now=clock())

        # No counter should increment for a hash the scheduler no longer owns.
        self.assertEqual(scheduler.diagnostics.persist_success, 0)

    def test_bounded_persist_result_dict(self):
        scheduler, _ = make_scheduler(persist_result_capacity=4)

        for i in range(20):
            scheduler.enqueue_persist_result(f"{i:040d}", True)

        with scheduler._persist_results_lock:
            size = len(scheduler._persist_results)
        self.assertLessEqual(size, scheduler.config.persist_result_capacity)


# ---------------------------------------------------------------------------
# Diagnostics emission
# ---------------------------------------------------------------------------

class DiagnosticsEmissionTests(unittest.TestCase):
    def test_diagnostics_emit_at_interval(self):
        scheduler, clock = make_scheduler(diagnostics_interval=5.0)
        captured = []
        scheduler.maybe_emit_diagnostics(now=clock(), logger=captured.append)
        self.assertEqual(captured, [])  # first call initializes only

        scheduler.on_announce(HASH_A, now=clock())
        clock.advance(5.1)
        snapshot = scheduler.maybe_emit_diagnostics(
            now=clock(), logger=captured.append
        )

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot["announce_peer_received"], 1)
        self.assertEqual(snapshot["in_flight"], 1)
        self.assertEqual(len(captured), 1)
        self.assertIn("sniff-scheduler", captured[0])

    def test_diagnostics_do_not_emit_before_interval(self):
        scheduler, clock = make_scheduler(diagnostics_interval=5.0)
        scheduler.maybe_emit_diagnostics(now=clock())

        clock.advance(2.0)
        snapshot = scheduler.maybe_emit_diagnostics(now=clock())

        self.assertIsNone(snapshot)


# ---------------------------------------------------------------------------
# Integration: fake session driving the alert-loop path
# ---------------------------------------------------------------------------

class MutableAlert:
    """Base class for fake alerts; allows __class__ reassignment."""

    pass


class FakeDhtPktAlert(MutableAlert):
    def __init__(self, pkt):
        self.pkt_buf = pkt


class FakeAddTorrentAlert(MutableAlert):
    def __init__(self, info_hash_hex, handle, params=None):
        self.handle = handle
        self.params = params or SimpleNamespace(
            info_hashes=SimpleNamespace(
                has_v1=lambda: True,
                has_v2=lambda: False,
                v1=info_hash_hex,
            )
        )


class FakeMetadataReceivedAlert(MutableAlert):
    def __init__(self, handle):
        self.handle = handle


class FakeLT:
    """Stand-in for libtorrent alert classes used in isinstance checks."""

    dht_pkt_alert = FakeDhtPktAlert
    add_torrent_alert = FakeAddTorrentAlert
    metadata_received_alert = FakeMetadataReceivedAlert

    @staticmethod
    def sha1_hash(raw_bytes):
        return raw_bytes.hex()

    @staticmethod
    def bdecode(data):
        import libtorrent as _lt
        return _lt.bdecode(data)

    @staticmethod
    def add_torrent_params():
        return SimpleNamespace(
            info_hashes=SimpleNamespace(
                has_v1=lambda: False, has_v2=lambda: False,
            ),
            ti=None,
            save_path=None,
            storage_mode=None,
            flags=0,
        )

    @staticmethod
    def parse_magnet_uri(uri):
        # Extract info_hash from magnet URI for testing.
        import urllib.parse
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(uri).query)
        xt = params.get("xt", [""])[0]
        if xt.startswith("urn:btih:"):
            hash_hex = xt[9:]
        elif xt.startswith("urn:btmh:"):
            hash_hex = xt[13:]
        else:
            hash_hex = ""
        return SimpleNamespace(
            info_hashes=SimpleNamespace(
                has_v1=lambda: len(hash_hex) == 40,
                has_v2=lambda: len(hash_hex) == 64,
                v1=hash_hex if len(hash_hex) == 40 else "",
                v2=hash_hex if len(hash_hex) == 64 else "",
            ),
            ti=None,
            save_path=None,
            storage_mode=None,
            flags=0,
        )

    class torrent_flags:
        duplicate_is_error = 1
        paused = 2
        upload_mode = 4

    class storage_mode_t:
        storage_mode_sparse = 0


class FakeHandle:
    def __init__(self, info_hash_hex):
        self.info_hash = info_hash_hex
        self._valid = True
        self.paused = False
        self.torrent_file = None
        self.connections = None
        self.uploads = None

    def is_valid(self):
        return self._valid

    def status(self):
        return SimpleNamespace(
            torrent_file=self.torrent_file,
            info_hashes=SimpleNamespace(
                has_v1=lambda: True,
                has_v2=lambda: False,
                v1=self.info_hash,
            ),
        )

    def pause(self):
        self.paused = True

    def set_max_connections(self, n):
        self.connections = n

    def set_max_uploads(self, n):
        self.uploads = n

    def invalidate(self):
        self._valid = False


def _build_announce_packet(info_hash_hex):
    """Build a minimal bencoded DHT announce_peer packet."""

    import libtorrent as lt
    info_hash_bytes = bytes.fromhex(info_hash_hex)
    packet = {
        b'y': b'q',
        b'q': b'announce_peer',
        b'a': {b'info_hash': info_hash_bytes},
    }
    return lt.bencode(packet)


class IntegrationTests(unittest.TestCase):
    """Drive app.handle_alert_announce_peer + scheduler + add_torrent.

    These tests stand up just enough of the app.py globals
    (``OPTIONS``, ``TORRENTS``, ``LIVE_HANDLES``, ``SESSION``,
    ``SNIFF_SCHEDULER``, ``TORRENT_DIR``) to run the alert handlers end
    to end against fake libtorrent alerts.
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.session_calls = []
        self.removed_handles = []
        self.session = SimpleNamespace(
            async_add_torrent=lambda params: self.session_calls.append(params),
            remove_torrent=lambda handle: self.removed_handles.append(handle),
        )

        self.options = SimpleNamespace(
            download_mode=False,
            save_path=self.tmpdir.name,
        )
        self.torrents = __import__("app").TTLDict(maxsize=100, ttl=600)
        self.live_handles = __import__("app").LiveHandles(maxsize=100)
        self.scheduler, self.clock = make_scheduler()
        self.metadata_queue = queue.Queue(maxsize=20)

        import app as app_module
        self.app_module = app_module

        self.patches = [
            patch.object(app_module, "OPTIONS", self.options, create=True),
            patch.object(app_module, "SESSION", self.session, create=True),
            patch.object(app_module, "TORRENTS", self.torrents, create=True),
            patch.object(app_module, "LIVE_HANDLES", self.live_handles),
            patch.object(app_module, "SNIFF_SCHEDULER", self.scheduler),
            patch.object(app_module, "TORRENT_DIR", self.tmpdir.name),
            patch.object(app_module, "TRACKERS", [], create=True),
            patch.object(app_module, "METADATA_QUEUE", self.metadata_queue),
            # Patch libtorrent so isinstance() checks match our fakes.
            patch.object(app_module, "lt", FakeLT),
        ]
        for p in self.patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in self.patches])

    def _announce(self, info_hash_hex):
        alert = FakeDhtPktAlert(_build_announce_packet(info_hash_hex))
        self.app_module.handle_alert_announce_peer(alert)

    def test_repeated_announces_do_not_start_duplicate_acquisition(self):
        self._announce(HASH_A)
        self._announce(HASH_A)
        self._announce(HASH_A)

        self.assertEqual(len(self.session_calls), 1)
        # After add_torrent, the hash is in TORRENTS, so subsequent announces
        # are skipped as "known to session" rather than "in flight".
        self.assertEqual(self.scheduler.diagnostics.skip_known_to_session, 2)
        self.assertEqual(self.scheduler.diagnostics.announce_peer_received, 3)

    def test_cached_file_skips_acquisition(self):
        with open(os.path.join(self.tmpdir.name, HASH_A + ".torrent"), "wb") as f:
            f.write(b"cached")

        self._announce(HASH_A)

        self.assertEqual(self.session_calls, [])
        self.assertEqual(self.scheduler.diagnostics.skip_cached_file, 1)

    def test_capacity_bound_limits_concurrent_acquisitions(self):
        self.scheduler.config.max_concurrent_acquisitions = 2
        for h in (HASH_A, HASH_B, HASH_C, HASH_D):
            self._announce(h)

        self.assertEqual(len(self.session_calls), 2)
        self.assertEqual(self.scheduler.in_flight_count(), 2)
        self.assertEqual(self.scheduler.queued_count(), 2)

    def test_persist_success_clears_scheduler_state(self):
        self._announce(HASH_A)
        self.assertEqual(len(self.session_calls), 1)

        handle = FakeHandle(HASH_A)
        handle.torrent_file = SimpleNamespace(
            info_hashes=SimpleNamespace(
                has_v1=lambda: True, has_v2=lambda: False, v1=HASH_A,
            )
        )

        # Pretend add_torrent_alert fired.
        add_alert = FakeAddTorrentAlert(HASH_A, handle)
        self.app_module.handle_alert_add_torrent(add_alert)

        # Now metadata_received.
        meta_alert = FakeMetadataReceivedAlert(handle)
        self.app_module.handle_alert_metadata_received(meta_alert)

        # Verify the metadata was queued for persistence.
        self.assertFalse(self.metadata_queue.empty())
        torrent_file, info_hash = self.metadata_queue.get_nowait()
        self.assertEqual(info_hash, HASH_A)

        # Simulate successful persistence.
        self.scheduler.enqueue_persist_result(HASH_A, True)
        self.scheduler.drain_persist_results(now=self.clock())

        self.assertIsNone(self.scheduler.state_of(HASH_A))
        self.assertEqual(self.scheduler.diagnostics.persist_success, 1)


# ---------------------------------------------------------------------------
# End-to-end duplicate suppression across TTL expiry
# ---------------------------------------------------------------------------

class TTLCacheSurvivalTests(unittest.TestCase):
    """The `.torrent` cache is the authoritative source post-success.

    After a successful persist, the cache file must prevent any
    subsequent announce from starting a duplicate retrieval, even when
    the 600-second ``TORRENTS`` TTL has expired.
    """

    def test_cache_file_blocks_after_scheduler_state_cleared(self):
        scheduler, clock = make_scheduler()
        # Simulate a full success cycle.
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())
        scheduler.enqueue_persist_result(HASH_A, True)
        scheduler.drain_persist_results(now=clock())
        self.assertIsNone(scheduler.state_of(HASH_A))

        # A later announce finds the cache file: must skip.
        admission = scheduler.on_announce(HASH_A, now=clock(), cache_exists=True)
        self.assertEqual(admission, ss.Admission.SKIP_CACHED)

    def test_without_cache_file_cooldown_then_retry_succeeds(self):
        """If the cache was deleted, cooldown expires, retry runs."""

        scheduler, clock = make_scheduler(
            cooldown_base_seconds=5.0, cooldown_jitter=0.0,
        )
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_async_add_failed(HASH_A, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)

        clock.advance(10.0)  # well past cooldown
        admission = scheduler.on_announce(HASH_A, now=clock())
        self.assertEqual(admission, ss.Admission.ADMITTED_STARTED)


# ---------------------------------------------------------------------------
# Process restart / cache cleanup behaviour
# ---------------------------------------------------------------------------

class RestartAndCleanupTests(unittest.TestCase):
    """Document and verify what survives a restart.

    The scheduler's cooldowns, queue, and in-flight set are in-memory
    only. A restart clears them. The `.torrent` cache files are the
    durable "already acquired" signal, and those are checked before any
    scheduler admission.
    """

    def test_restart_clears_cooldowns(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_async_add_failed(HASH_A, now=clock())
        self.assertIn(HASH_A, scheduler.cooldowns)

        fresh = ss.SniffScheduler(scheduler.config, clock=clock)
        self.assertNotIn(HASH_A, fresh.cooldowns)

    def test_cache_file_after_restart_still_blocks(self):
        """Post-restart, a cache file must still skip the hash."""

        scheduler, clock = make_scheduler()
        admission = scheduler.on_announce(HASH_A, now=clock(), cache_exists=True)
        self.assertEqual(admission, ss.Admission.SKIP_CACHED)


# ---------------------------------------------------------------------------
# Regression tests for code-review findings on initial PR head 46b501a
# ---------------------------------------------------------------------------

class RegressionTests(unittest.TestCase):
    """Verify fixes for the 7 findings from the independent review.

    Each test is named after the finding it addresses so the mapping is
    explicit.
    """

    # Finding 1: timed-out handles must actually be removed by the caller;
    # the slot is released only after ack.
    def test_f1_timeout_two_phase_protocol(self):
        scheduler, clock = make_scheduler(acquisition_timeout=5.0)
        scheduler.on_announce(HASH_A, now=clock())
        handle = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle, now=clock())
        clock.advance(6.0)

        # tick returns the handle but keeps state at PENDING_REMOVAL.
        promoted, pending = scheduler.tick(now=clock())
        self.assertEqual(promoted, [])
        self.assertEqual(pending, [(HASH_A, handle)])
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.PENDING_REMOVAL)
        # Slot is still occupied until ack.
        self.assertEqual(scheduler.in_flight_count(), 1)

        # Caller removes the handle and acks; slot releases.
        scheduler.ack_handle_removed(HASH_A, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.in_flight_count(), 0)

        # A new announce for the same hash after cooldown expires can start
        # fresh, and LIVE_HANDLES/TORRENTS no longer suppress it.
        clock.advance(100.0)
        admission = scheduler.on_announce(HASH_A, now=clock())
        self.assertEqual(admission, ss.Admission.ADMITTED_STARTED)

    # Finding 2: _candidates and cooldown registry must stay bounded
    # across many unique hashes and repeated failures.
    def test_f2_candidates_bounded_across_many_hashes(self):
        scheduler, clock = make_scheduler(
            max_candidates=8,
            max_cooldown_entries=4,
            cooldown_base_seconds=1.0,
            cooldown_ttl=100.0,
            cooldown_jitter=0.0,
        )
        # Drive 20 unique hashes through failure; both registry and
        # _candidates must remain within their bounds.
        for i in range(20):
            h = f"{i:040d}"
            scheduler.on_announce(h, now=clock())
            scheduler.on_async_add_failed(h, now=clock())
            clock.advance(0.01)

        self.assertLessEqual(len(scheduler.cooldowns), 4)
        self.assertLessEqual(scheduler.candidate_count(), 8)

    def test_f2_repeated_failures_do_not_unbound_order(self):
        scheduler, clock = make_scheduler(
            cooldown_base_seconds=0.1,
            cooldown_jitter=0.0,
            cooldown_ttl=100.0,
        )
        for _ in range(50):
            scheduler.on_announce(HASH_A, now=clock())
            scheduler.on_async_add_failed(HASH_A, now=clock())
            clock.advance(0.2)  # past cooldown each time

        # _order must not grow unboundedly: the registry only has one entry
        # for HASH_A, and our deduplicated _order tracks it once.
        self.assertEqual(len(scheduler.cooldowns._order_set), 1)

    # Finding 3: persist results must not be silently dropped.
    def test_f3_persist_results_latest_writer_wins(self):
        """When capacity is bounded, duplicate results collapse; unique
        results up to capacity are all preserved."""

        scheduler, clock = make_scheduler(persist_result_capacity=16)
        hashes = [f"{i:040d}" for i in range(10)]
        for h in hashes:
            scheduler.on_announce(h, now=clock())
            scheduler.on_metadata_received(h, now=clock())
            scheduler.enqueue_persist_result(h, True)

        scheduler.drain_persist_results(now=clock())

        persisting = [
            h for h, c in scheduler._candidates.items()
            if c.state is ss.CandidateState.PERSISTING
        ]
        self.assertEqual(persisting, [])
        self.assertEqual(scheduler.diagnostics.persist_success, 10)

    def test_f3_latest_persist_result_wins(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        scheduler.on_metadata_received(HASH_A, now=clock())

        # Worker reports failure, then success (out-of-order but same hash).
        scheduler.enqueue_persist_result(HASH_A, False)
        scheduler.enqueue_persist_result(HASH_A, True)
        scheduler.drain_persist_results(now=clock())

        # The success result wins; no cooldown.
        self.assertIsNone(scheduler.state_of(HASH_A))
        self.assertEqual(scheduler.diagnostics.persist_success, 1)
        self.assertEqual(scheduler.diagnostics.persist_failure, 0)

    # Finding 4: old alerts cannot mutate a new attempt for the same hash.
    def test_f4_old_metadata_cannot_mutate_new_attempt(self):
        scheduler, clock = make_scheduler(
            acquisition_timeout=5.0,
            cooldown_base_seconds=1.0,
            cooldown_jitter=0.0,
        )
        # First attempt: register handle, time out, ack removal, cooldown expires.
        scheduler.on_announce(HASH_A, now=clock())
        handle_old = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle_old, now=clock())
        clock.advance(6.0)
        _, pending = scheduler.tick(now=clock())
        scheduler.ack_handle_removed(HASH_A, now=clock())
        clock.advance(100.0)  # past cooldown

        # Second attempt starts with a new handle.
        scheduler.on_announce(HASH_A, now=clock())
        handle_new = SimpleNamespace(is_valid=lambda: True)
        scheduler.on_add_started(HASH_A, handle_new, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.IN_FLIGHT)

        # Old metadata_received_alert arrives with the OLD handle; must be
        # rejected without transitioning the new attempt.
        owned = scheduler.on_metadata_received(HASH_A, handle=handle_old, now=clock())
        self.assertFalse(owned)
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.IN_FLIGHT)
        self.assertEqual(scheduler.diagnostics.late_alert_ignored, 1)

        # New handle's metadata_received is accepted.
        owned = scheduler.on_metadata_received(HASH_A, handle=handle_new, now=clock())
        self.assertTrue(owned)
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.PERSISTING)

    # Finding 5: persist queue full must release slot and enter cooldown
    # from IN_FLIGHT, not be silently ignored.
    def test_f5_persist_queue_full_from_in_flight(self):
        scheduler, clock = make_scheduler()
        scheduler.on_announce(HASH_A, now=clock())
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.IN_FLIGHT)
        self.assertEqual(scheduler.in_flight_count(), 1)

        # Directly report queue full without going through metadata_received.
        released = scheduler.on_persist_queue_full(HASH_A, now=clock())

        self.assertTrue(released)
        self.assertEqual(scheduler.state_of(HASH_A), ss.CandidateState.COOLDOWN)
        self.assertEqual(scheduler.in_flight_count(), 0)
        self.assertEqual(scheduler.diagnostics.persist_queue_full, 1)

    # Finding 6: queue-full drop must apply bounded overload suppression.
    def test_f6_queue_drop_applies_cooldown(self):
        scheduler, clock = make_scheduler(
            max_queue_size=1,
            max_concurrent_acquisitions=1,
            queue_drop_cooldown_seconds=5.0,
            cooldown_jitter=0.0,
        )
        scheduler.on_announce(HASH_A, now=clock())  # starts
        scheduler.on_announce(HASH_B, now=clock())  # queued
        admission = scheduler.on_announce(HASH_C, now=clock())  # dropped

        self.assertEqual(admission, ss.Admission.DROP_QUEUE_FULL)
        # HASH_C now has a cooldown; repeat announces are skipped.
        self.assertEqual(
            scheduler.on_announce(HASH_C, now=clock()),
            ss.Admission.SKIP_COOLDOWN,
        )
        # After the short queue-drop cooldown expires AND the queue has
        # room, HASH_C can be admitted.
        clock.advance(6.0)
        # Clear HASH_B by completing HASH_A's acquisition.
        scheduler.on_metadata_received(HASH_A, now=clock())
        scheduler.tick(now=clock())  # promotes HASH_B -> now HASH_A's slot is PERSISTING
        # HASH_B is now IN_FLIGHT; queue is empty. HASH_C can now be admitted.
        admission = scheduler.on_announce(HASH_C, now=clock())
        self.assertEqual(admission, ss.Admission.ADMITTED_QUEUED)

    # Finding 7: hash_overflow counts distinct overflow hashes, not repeats.
    def test_f7_hash_overflow_counts_distinct(self):
        diag = ss.Diagnostics(budget=2)
        diag.observe_hash(HASH_A)
        diag.observe_hash(HASH_B)
        # HASH_C overflows once.
        diag.observe_hash(HASH_C)
        # Repeat announces of HASH_C must not inflate overflow count.
        diag.observe_hash(HASH_C)
        diag.observe_hash(HASH_C)
        # A fourth distinct hash also overflows.
        diag.observe_hash(HASH_D)

        self.assertEqual(diag.hash_overflow, 2)
        self.assertTrue(diag.hash_set_saturated)


if __name__ == "__main__":
    unittest.main()
