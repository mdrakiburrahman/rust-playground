"""Source observer projection tests; fake Engine transport, not real qualification."""
import copy
import hashlib
import json
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

    def test_effect_identity_probe_does_not_wait_for_telemetry(self):
        original = self.observer.engine.get
        self.observer.engine.get = Mock(side_effect=original)
        _, _, running, stats, _ = self.observer.component(("collector1", "collector1"), with_stats=False)
        self.assertTrue(running)
        self.assertIsNone(stats)
        self.assertEqual(len(self.observer.engine.get.call_args_list), 1)
        self.assertTrue(self.observer.engine.get.call_args.args[0].endswith("/json"))

    def test_receipt_component_failure_keeps_prior_identities_but_marks_unknown(self):
        receipt = {"id": "receipt-1", "table_id": "main", "operation_id": "unit-1",
                   "version": "0", "completion": "completed", "digest": "a" * 64}
        self.observer.receipt_probe = Mock(side_effect=[
            ([receipt], [], 0), RuntimeError("private-diagnostic"), ([receipt], [], 0)])
        self.observer.sample()
        self.assertNotIn("receipts", self.packet()["coverage"]["missing"])
        self.assertEqual(self.packet()["receipts"][0]["completion"], "completed")
        self.observer.sample()
        self.assertEqual(self.packet()["receipts"][0]["operation_id"], "unit-1")
        self.assertEqual(self.packet()["receipts"][0]["completion"], "unknown")
        self.assertIn("receipts", self.packet()["coverage"]["missing"])
        self.assertIn("receipt-observation-error", self.packet()["coverage"]["missing"])
        self.assertEqual(self.packet()["coverage"]["dropped"], 1)
        self.observer.sample()
        self.assertEqual(self.packet()["receipts"][0]["completion"], "completed")

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
                    patch.object(live_runtime.os, "umask", return_value=0o022), \
                    patch.object(live_runtime.signal, "signal"), patch("sys.argv", arguments), \
                    self.assertRaisesRegex(RuntimeError, "bounded startup failure"):
                live_runtime.main()
            runtime.finish_runtime.assert_called_once_with()

    def test_restrictive_umask_is_rejected_before_native_runtime_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "index.html").write_text("test")
            arguments = ["live_runtime", "--project", "publication-gate-test", "--subnet", "10.235.180.0/24",
                         "--arm", "adr", "--port", "18765", "--frontend-root", str(root),
                         "--bootstrap-file", str(root.parent / (root.name + "-bootstrap"))]
            with patch.object(live_runtime, "Runtime") as runtime, \
                    patch.object(live_runtime, "require_local_images"), \
                    patch.object(live_runtime.os, "umask", return_value=0o077), \
                    patch.object(live_runtime.signal, "signal"), patch("sys.argv", arguments), \
                    self.assertRaisesRegex(ValueError, "umask 022"):
                live_runtime.main()
            runtime.assert_not_called()

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

    def test_policy_enablement_requires_matching_bounded_real_proof_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "target/debug").mkdir(parents=True)
            binary = root / "target/debug/qualified-binary"
            binary.write_bytes(b"synthetic qualified binary")
            sources = Path(live_runtime.__file__).parent
            proof = {
                "server_exit_code": 0, "exact_owned_containers_stopped": 8, "native_policy_mutations": 4,
                "ordinary_delta_exact_marker_rows": 9, "lost_response_reconciled_without_replay": True,
                "ledger_reopen": True, "admission_rejected_new_stream_before_receipt": True,
                "exclusion_did_not_block_owned_table_streams": True, "stale_policy_and_wrong_resource_denied": True,
                "sql_image": live_runtime.SQL_IMAGE, "runtime_image": live_runtime.RUNTIME_IMAGE,
                "source_sha256": {name: hashlib.sha256((sources / name).read_bytes()).hexdigest()
                                  for name in ("live_executor.py", "live_ledger.py", "live_contract.py",
                                               "contracts/live-v1.schema.json")},
                "shared_binaries": {"qualified-binary": hashlib.sha256(binary.read_bytes()).hexdigest()},
            }
            path = root / "proof.json"
            with patch.object(live_runtime, "REPO", root), patch.object(live_runtime, "BINARIES", ("qualified-binary",)):
                path.write_text(json.dumps(proof))
                self.assertEqual(live_runtime.verify_policy_proof(path), hashlib.sha256(path.read_bytes()).hexdigest())
                for field, invalid in (("server_exit_code", False), ("native_policy_mutations", 0),
                                       ("ledger_reopen", False), ("sql_image", "foreign"),
                                       ("source_sha256", {}), ("shared_binaries", {})):
                    changed = copy.deepcopy(proof)
                    changed[field] = invalid
                    path.write_text(json.dumps(changed))
                    with self.subTest(field=field), self.assertRaises(ValueError):
                        live_runtime.verify_policy_proof(path)
                path.write_bytes(b"x" * 262145)
                with self.assertRaisesRegex(ValueError, "exceeds bound"):
                    live_runtime.verify_policy_proof(path)

    def test_additional_effects_require_complete_matching_source_binary_and_native_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "target/debug").mkdir(parents=True)
            binary = root / "target/debug/qualified-binary"
            binary.write_bytes(b"synthetic qualified binary")
            sources = Path(live_runtime.__file__).parent
            names = ("live_effects.py", "live_executor.py", "live_faults.py", "live_workload.py", "live_ledger.py",
                     "live_server.py", "live_runtime.py", "live_deployment.py", "live_contract.py", "live_receipts.py",
                     "contracts/live-v1.schema.json")
            proof = {
                "server_exit_code": 0, "exact_owned_containers_stopped": 8,
                "ordinary_delta_exact_marker_rows": 65548, "bounded_single_attempts": 4, "arm": "adr",
                "server_owned_small_and_elephant_profiles": True, "same_id_no_replay_and_journal_reopen": True,
                "publisher_inflight_completion": True, "completion_requires_public_physical_receipt_observation": True,
                "measured_fault_lifecycle_below_requested_plus_5s": True,
                "actual_bridge_sigkill_independent_rollback": True, "actual_worker_sigkill_then_emergency_repair": True,
                "sql_image": live_runtime.SQL_IMAGE, "runtime_image": live_runtime.RUNTIME_IMAGE,
                "source_sha256": {name: hashlib.sha256((sources / name).read_bytes()).hexdigest() for name in names},
                "shared_binaries": {"qualified-binary": hashlib.sha256(binary.read_bytes()).hexdigest()},
            }
            path = root / "effects-proof.json"
            with patch.object(live_runtime, "REPO", root), patch.object(live_runtime, "BINARIES", ("qualified-binary",)):
                path.write_text(json.dumps(proof))
                self.assertEqual(live_runtime.verify_effects_proof(path), hashlib.sha256(path.read_bytes()).hexdigest())
                for field, invalid in (
                    ("ordinary_delta_exact_marker_rows", True), ("actual_bridge_sigkill_independent_rollback", False),
                    ("bounded_single_attempts", 0), ("source_sha256", {}), ("shared_binaries", {}),
                    ("runtime_image", "foreign"), ("arm", "pingora"),
                ):
                    changed = copy.deepcopy(proof)
                    changed[field] = invalid
                    path.write_text(json.dumps(changed))
                    with self.subTest(field=field), self.assertRaises(ValueError):
                        live_runtime.verify_effects_proof(path)
                http = {**proof, "proof_kind": "guarded-http-effects",
                        "physical_workload_units": 4, "combined_ordinary_delta_exact_marker_rows": 65554,
                        "native_policy_mutations": 4, "frontend_mutations": False,
                        "fault_owner_sigkill_exit": -9, "crashed_process_exit": -9,
                        "effects": [{"operation": operation, "status": "completed"}
                                    for operation in ["fault"] * 5 + ["workload"] * 3],
                        "fault_cases": [{"fault": variant, "phase": "repaired", "applied": True,
                                         "duration_ms": 500, "elapsed_ns": 600_000_000}
                                        for variant in ("pause", "authority-loss", "publisher-interrupt",
                                                        "pause", "pause")]}
                for name in (
                    "durable_same_id_reconciled_without_replay", "stable_deployment_new_bridge_incarnation",
                    "snapshot_revision_watermark_preserved", "old_session_denied", "wrong_resource_and_csrf_denied",
                    "independent_rollback_before_restart", "fault_and_workload_overlap",
                    "workload_lost_http_response_no_replay", "endpoint_closed",
                    "actual_parent_death_during_preflight_no_application",
                ):
                    http[name] = True
                path.write_text(json.dumps(http))
                self.assertEqual(live_runtime.verify_effects_proof(path), hashlib.sha256(path.read_bytes()).hexdigest())
                for field, invalid in (
                    ("physical_workload_units", True), ("fault_cases", []), ("effects", [None]),
                    ("combined_ordinary_delta_exact_marker_rows", 9), ("native_policy_mutations", False),
                    ("actual_parent_death_during_preflight_no_application", False),
                    ("actual_worker_sigkill_then_emergency_repair", False), ("source_sha256", {}),
                    ("endpoint_closed", False), ("fault_owner_sigkill_exit", 0),
                ):
                    changed = copy.deepcopy(http)
                    changed[field] = invalid
                    path.write_text(json.dumps(changed))
                    with self.subTest(http_field=field), self.assertRaises(ValueError):
                        live_runtime.verify_effects_proof(path)
                path.write_bytes(b"x" * 262145)
                with self.assertRaisesRegex(ValueError, "exceeds bound"):
                    live_runtime.verify_effects_proof(path)


if __name__ == "__main__":
    unittest.main()
