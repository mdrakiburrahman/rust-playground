import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from observation import Observer, cpu_interval


class ObservationTests(unittest.TestCase):
    def test_cpu_intervals_preserve_unknown_first_sample_and_reject_resets(self):
        previous = {"observer_clock_id": "clock-1", "observed_monotonic_ns": 100, "cpu_total_usage_ns": 50}
        self.assertIsNone(cpu_interval(None, previous))
        self.assertEqual(cpu_interval(previous, {"observer_clock_id": "clock-1", "observed_monotonic_ns": 200, "cpu_total_usage_ns": 70}),
                         {"elapsed_ns": 100, "cpu_usage_ns": 20, "one_core_percent": 20})
        for current in ({"observer_clock_id": "clock-1", "observed_monotonic_ns": 99, "cpu_total_usage_ns": 70},
                        {"observer_clock_id": "clock-1", "observed_monotonic_ns": 200, "cpu_total_usage_ns": 49}):
            with self.subTest(current=current), self.assertRaisesRegex(ValueError, "counter/clock"):
                cpu_interval(previous, current)

    def test_probe_cadence_uses_monotonic_time_when_wall_clock_moves_backward(self):
        with tempfile.TemporaryDirectory() as directory, patch("observation.Engine") as engine:
            engine.return_value.get.return_value = []
            probe = Mock(return_value={"snapshot": True})
            observer = Observer("publication-gate-clock-test", Path(directory) / "samples", probe=probe)
            try:
                with patch("observation.time.time_ns", side_effect=[100, 101, 50, 51]), \
                     patch("observation.time.monotonic_ns", side_effect=[10**9, 10**9 + 10, 2 * 10**9, 2 * 10**9 + 10]):
                    observer.sample()
                    observer.sample()
                self.assertEqual(probe.call_count, 2)
                records = [json.loads(line) for line in Path(observer.output.name).read_text().splitlines()]
                self.assertLess(records[1]["started_ns"], records[0]["started_ns"])
                self.assertGreater(records[1]["started_monotonic_ns"], records[0]["ended_monotonic_ns"])
            finally:
                observer.executor.shutdown(wait=True)
                observer.output.close()

    def test_invalid_counter_interval_is_retained_as_sampling_error(self):
        with tempfile.TemporaryDirectory() as directory, patch("observation.Engine") as engine:
            engine.return_value.get.return_value = [{}]
            observer = Observer("publication-gate-counter-test", Path(directory) / "samples")
            observer.previous["owned-container"] = {
                "observer_clock_id": "clock-1", "observed_monotonic_ns": 100, "cpu_total_usage_ns": 50}
            try:
                with patch.object(observer, "_container", return_value={
                    "id": "owned-container", "observer_clock_id": "clock-1",
                    "observed_monotonic_ns": 200, "cpu_total_usage_ns": 40,
                }):
                    observer.sample()
                self.assertEqual(observer.errors, 1)
                record = json.loads(Path(observer.output.name).read_text())
                self.assertIn("invalid CPU counter/clock interval", record["error"])
                self.assertIsNone(record["containers"][0]["cpu_interval"])
                self.assertEqual(record["containers"][0]["cpu_interval_error"], "invalid-counter-interval")
                self.assertNotIn("owned-container", observer.previous)
            finally:
                observer.executor.shutdown(wait=True)
                observer.output.close()

    def test_explicit_partial_observation_stop_preserves_errors_in_summary(self):
        with tempfile.TemporaryDirectory() as directory, patch("observation.Engine") as engine:
            engine.return_value.get.return_value = []
            observer = Observer("publication-gate-partial-test", Path(directory) / "samples")
            observer.thread = Mock()
            observer.thread.is_alive.return_value = False
            observer.started_ns = 0
            observer.errors = 1
            summary = observer.stop(strict=False)
            self.assertEqual(summary["sampling_errors"], 1)
            self.assertTrue(observer.output.closed)
