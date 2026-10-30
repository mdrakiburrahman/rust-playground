"""Source observer projection tests; fake Engine transport, not real qualification."""
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_contract import validate_packet
import live_runtime
from live_runtime import RuntimeObserver


class RuntimeProjectionTests(unittest.TestCase):
    def setUp(self):
        self.state = {
            "Id": "owned-id", "Config": {"Labels": {"delta-runtime": "test-project"}},
            "State": {"Running": True, "StartedAt": "test-start"},
        }
        self.stats = {"cpu_stats": {"cpu_usage": {"total_usage": 10}},
                      "memory_stats": {"usage": 4096}}
        self.writer = {"collector": "collector1", "process": "test-process", "generation": "test-generation"}
        self.authority = {
            "members": [{"writer": self.writer, "eligible": True}],
            "policies": {}, "assignments": [],
        }
        runtime = SimpleNamespace(
            project="test-project", names={"collector1": "collector1"}, sql_name="sql",
            owned_container=lambda _: "owned-id", config={"tables": [{"key": "main"}]}, control_role="controller",
        )
        engine = SimpleNamespace(get=lambda path: copy.deepcopy(
            self.state if path.endswith("/json") else self.stats))
        self.bridge = SimpleNamespace(publish=Mock())
        self.observer = RuntimeObserver(runtime, self.bridge, engine)
        self.observer.authority = lambda: copy.deepcopy(self.authority)

    def tearDown(self):
        self.observer.close()

    def packet(self):
        replicas, tables, receipts, metrics, missing = self.bridge.publish.call_args.args
        return validate_packet({
            "kind": "snapshot", "schema_version": 1,
            "bridge": {"id": "test-bridge", "incarnation": "test-incarnation"},
            "deployment": {"id": "test-project", "incarnation": "test-deployment"},
            "revision": "1", "freshness": {"status": "fresh", "age_ns": "0", "maximum_age_ns": "5000000000"},
            "replicas": replicas, "tables": tables, "receipts": receipts, "metrics": metrics,
            "coverage": {"complete": False, "missing": missing,
                         "dropped": self.bridge.publish.call_args.kwargs.get("dropped", 0)},
        })

    def test_counters_and_missing_receipts_are_explicit_and_bounded(self):
        self.observer.sample()
        packet = self.packet()
        cpu = next(metric for metric in packet["metrics"] if metric["metric"] == "cpu")
        self.assertIsNone(cpu["value"])
        self.assertEqual(cpu["evidence"], "unavailable")
        self.assertIn("receipts", packet["coverage"]["missing"])
        self.stats["cpu_stats"]["cpu_usage"]["total_usage"] = 30
        self.observer.sample()
        packet = self.packet()
        cpu = next(metric for metric in packet["metrics"] if metric["metric"] == "cpu")
        self.assertEqual(cpu["value"], "20")
        self.assertGreater(int(cpu["duration_ns"]), 0)
        self.assertNotIn("owned-id", str(packet))

    def test_restart_changes_resource_incarnation_and_resets_counter_interval(self):
        self.observer.sample()
        before = self.packet()["replicas"][0]["resource"]
        self.state["State"]["StartedAt"] = "replacement-process"
        self.stats["cpu_stats"]["cpu_usage"]["total_usage"] = 1
        self.observer.sample()
        packet = self.packet()
        self.assertNotEqual(before, packet["replicas"][0]["resource"])
        self.assertIsNone(next(metric["value"] for metric in packet["metrics"] if metric["metric"] == "cpu"))

    def test_foreign_resource_and_foreign_policy_are_rejected(self):
        self.state["Config"]["Labels"]["delta-runtime"] = "foreign"
        with self.assertRaisesRegex(ValueError, "identity"):
            self.observer.sample()
        self.state["Config"]["Labels"]["delta-runtime"] = "test-project"
        self.authority["policies"]["collector1"] = {"incarnation": {**self.writer, "process": "foreign"}}
        with self.assertRaisesRegex(ValueError, "incarnation"):
            self.observer.sample()

    def test_unknown_memory_does_not_turn_into_observed_zero(self):
        self.stats["memory_stats"].clear()
        self.observer.sample()
        packet = self.packet()
        memory = next(metric for metric in packet["metrics"] if metric["metric"] == "memory")
        self.assertIsNone(memory["value"])
        self.assertEqual(memory["method"], "not-observed")

    def test_malformed_first_cpu_counter_is_not_saved_for_later_intervals(self):
        self.stats["cpu_stats"]["cpu_usage"]["total_usage"] = True
        with self.assertRaisesRegex(ValueError, "CPU counter"):
            self.observer.sample()
        self.assertEqual(self.observer.previous, {})

    def test_counter_reset_retains_memory_and_breaks_cpu_continuity(self):
        self.observer.sample()
        self.stats["cpu_stats"]["cpu_usage"]["total_usage"] = 1
        self.observer.sample()
        packet = self.packet()
        self.assertIn("cpu-counter-gap", packet["coverage"]["missing"])
        self.assertGreater(packet["coverage"]["dropped"], 0)
        self.assertTrue(all(metric["value"] is None for metric in packet["metrics"] if metric["metric"] == "cpu"))
        self.assertTrue(all(metric["value"] == "4096" for metric in packet["metrics"] if metric["metric"] == "memory"))
        self.assertEqual(self.observer.previous, {})
        self.stats["cpu_stats"]["cpu_usage"]["total_usage"] = 100
        self.observer.sample()
        self.assertTrue(all(metric["value"] is None for metric in self.packet()["metrics"] if metric["metric"] == "cpu"))

    def test_recovered_observation_retains_dropped_sample_count(self):
        self.observer.dropped = 2
        self.observer.sample()
        packet = self.packet()
        self.assertEqual(packet["coverage"]["dropped"], 2)
        self.assertIn("observation-gap", packet["coverage"]["missing"])
        self.assertEqual(self.observer.dropped, 0)

    def test_retirement_approval_is_not_teardown_completion(self):
        self.authority["policies"]["collector1"] = {
            "incarnation": self.writer, "revision": 3, "applied_revision": 3,
            "retiring": True, "approved": True, "excluded": True, "block_streams": True,
        }
        self.observer.sample()
        collector = next(replica for replica in self.packet()["replicas"] if replica["role"] == "collector")
        self.assertEqual(collector["status"], "draining")
        self.state["State"]["Running"] = False
        self.observer.sample()
        collector = next(replica for replica in self.packet()["replicas"] if replica["role"] == "collector")
        self.assertEqual(collector["status"], "retired")

    def test_bounded_engine_failure_does_not_kill_observer_reconnection(self):
        original = self.observer.sample
        calls = []
        def sample():
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("synthetic bounded Engine failure")
            original()
            self.observer.stop.set()
        self.observer.sample = sample
        self.observer.start()
        self.observer.thread.join(timeout=3)
        self.assertFalse(self.observer.thread.is_alive())
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.packet()["coverage"]["dropped"], 1)

    def test_runtime_evidence_is_private_before_credentials_are_created(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            frontend = root / "frontend"
            frontend.mkdir()
            (frontend / "index.html").write_text("test")
            evidence = root / "runtime"
            evidence.mkdir(mode=0o755)
            def setup():
                self.assertEqual(evidence.stat().st_mode & 0o777, 0o700)
                raise RuntimeError("bounded startup failure")
            runtime = SimpleNamespace(root=evidence, setup_runtime=setup, finish_runtime=Mock())
            arguments = ["live_runtime", "--project", "publication-gate-test", "--subnet", "10.235.180.0/24",
                         "--arm", "adr", "--port", "18765", "--frontend-root", str(frontend),
                         "--bootstrap-file", str(root / "private-bootstrap")]
            with patch.object(live_runtime, "Runtime", return_value=runtime), \
                    patch.object(live_runtime, "require_local_images"), \
                    patch.object(live_runtime.signal, "signal"), patch("sys.argv", arguments), \
                    self.assertRaisesRegex(RuntimeError, "bounded startup failure"):
                live_runtime.main()
            runtime.finish_runtime.assert_called_once_with()

    def test_explicit_local_image_pins_prevent_implicit_legacy_image_pulls(self):
        sql = "sha256:" + "a" * 64
        native = "ghcr.io/example/runtime@sha256:" + "b" * 64
        pins = {"ARROWCOL_SQL_IMAGE": sql, "ARROWCOL_RUNTIME_IMAGE": native}
        with patch.object(live_runtime, "SQL_IMAGE", sql), patch.object(live_runtime, "RUNTIME_IMAGE", native), \
                patch.object(live_runtime.subprocess, "run") as run:
            with patch.dict(live_runtime.os.environ, {}, clear=True), \
                    self.assertRaisesRegex(ValueError, "explicit immutable"):
                live_runtime.require_local_images()
            run.assert_not_called()
            with patch.dict(live_runtime.os.environ, pins):
                run.return_value = SimpleNamespace(returncode=1, stdout="")
                with self.assertRaisesRegex(ValueError, "never pulls"):
                    live_runtime.require_local_images()
                run.return_value = SimpleNamespace(returncode=0, stdout=sql + "\n")
                live_runtime.require_local_images()
            self.assertTrue(all(call.args[0][:3] == ["docker", "image", "inspect"] for call in run.call_args_list))


if __name__ == "__main__":
    unittest.main()
