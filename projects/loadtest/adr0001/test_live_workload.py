"""Synthetic workload transport fixtures, not real native workload qualification."""
import copy
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_contract import Denial
from live_ledger import OperationLedger
from live_workload import NativeWorkload, PRESETS, WorkloadRunner, preset_for


class WorkloadTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "operations.sqlite"
        self.ledger = OperationLedger(self.path)
        self.deployment = {"id": "poc", "incarnation": "deployment-1"}
        self.packet = {
            "kind": "command", "schema_version": 1, "operation_id": "workload-1",
            "selection_id": "selection-1", "arm_id": "arm-1", "deployment": self.deployment, "revision": "7",
            "action": {"operation": "workload", "profile": "small", "requests": 1, "duration_ms": 1000, "streams": 1},
        }
        self.snapshot = {
            "kind": "snapshot", "schema_version": 1,
            "bridge": {"id": "bridge", "incarnation": "bridge-1"}, "deployment": self.deployment,
            "revision": "8", "freshness": {"status": "fresh", "age_ns": "0", "maximum_age_ns": "5000000000"},
            "replicas": [], "tables": [{"id": "main", "epoch": "0", "lifecycle": "unknown",
                                       "writer": None, "recovery_obligations": []}], "receipts": [], "metrics": [],
            "coverage": {"complete": False, "missing": ["cpu"], "dropped": 0},
            "receipt_observation_known": True,
        }
        self.sent = []
        self.unit = "11111111-1111-4111-8111-111111111111"
        self.prepare_count = 0
        self.mode = "final"
        self.native = SimpleNamespace(prepare=self.prepare, send=self.send)
        self.runner = WorkloadRunner(self.ledger, self.native, lambda: copy.deepcopy(self.snapshot))

    def tearDown(self):
        self.ledger.close()
        self.directory.cleanup()

    def prepare(self):
        self.prepare_count += 1

    def send(self, attempt, remaining, remember):
        import pyarrow.flight as flight
        self.assertGreater(remaining, 0)
        self.assertLessEqual(remaining, 1)
        self.assertTrue(self.runner.journal.get("workload-1", self.deployment)["attempts"][0]["sent"])
        self.sent.append(copy.deepcopy(attempt))
        if self.mode == "missing":
            raise TimeoutError("synthetic response lost before unit identity")
        if self.mode == "flight-missing":
            raise flight.FlightUnavailableError("synthetic unavailable before unit identity")
        remember(unit=self.unit)
        if self.mode == "timeout":
            raise TimeoutError("synthetic response lost after unit identity")
        if self.mode == "flight-timeout":
            raise flight.FlightTimedOutError("synthetic native flight timeout after unit identity")
        remember(final={"version": "2", "digest": "a" * 64}, rpc_status="final")

    def receipt(self, completion="completed"):
        self.snapshot["receipts"] = [{
            "id": "receipt-2", "table_id": "main", "operation_id": self.unit,
            "version": "2", "completion": completion, "digest": "a" * 64,
        }]

    def run_job(self, authorize=lambda: None, stopping=lambda: False):
        self.ledger.reserve(self.packet)
        return self.runner.run("workload-1", self.deployment, authorize, lambda: None, stopping)

    def test_rpc_final_and_sql_acceptance_never_substitute_for_physical_completion(self):
        self.assertEqual(self.run_job(), ("unknown", None))
        job = self.runner.journal.get("workload-1", self.deployment)
        self.assertEqual(job["attempts"][0]["rpc_status"], "final")
        self.assertEqual(job["attempts"][0]["unit"], self.unit)
        self.assertNotEqual(job["attempts"][0]["request_id"], job["attempts"][0]["attempt_id"])
        self.receipt("accepted")
        self.assertEqual(self.runner.outcome("workload-1", self.deployment), ("unknown", None))
        self.receipt()
        self.assertEqual(self.runner.outcome("workload-1", self.deployment), ("completed", "8"))
        self.assertEqual(self.run_job(), ("completed", "8"))
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.prepare_count, 1)

    def test_reopen_and_lost_rpc_response_reconcile_without_replay(self):
        self.mode = "timeout"
        self.assertEqual(self.run_job(), ("unknown", None))
        self.ledger.close()
        self.ledger = OperationLedger(self.path)
        self.runner = WorkloadRunner(self.ledger, self.native, lambda: copy.deepcopy(self.snapshot))
        self.receipt()
        self.assertEqual(self.runner.outcome("workload-1", self.deployment), ("completed", "8"))
        self.assertEqual(self.run_job(), ("completed", "8"))
        self.assertEqual(len(self.sent), 1)
        attempt = self.runner.journal.get("workload-1", self.deployment)["attempts"][0]
        self.assertEqual(attempt["rpc_status"], "timeout")
        self.assertEqual(attempt["error_code"], "timeout")
        self.assertIsNone(attempt["final"])

    def test_missing_native_unit_stays_unknown_and_cannot_replay(self):
        self.mode = "missing"
        self.assertEqual(self.run_job(), ("unknown", None))
        self.receipt()
        self.assertEqual(self.run_job(), ("unknown", None))
        self.assertEqual(len(self.sent), 1)
        with self.assertRaises(ValueError):
            self.runner.journal.update("workload-1", self.deployment, 0, sent=False)

    def test_native_flight_error_hierarchy_is_preserved_and_can_complete_later(self):
        self.mode = "flight-timeout"
        self.assertEqual(self.run_job(), ("unknown", None))
        attempt = self.runner.journal.get("workload-1", self.deployment)["attempts"][0]
        self.assertEqual((attempt["rpc_status"], attempt["error_code"]), ("timeout", "timeout"))
        self.receipt()
        self.assertEqual(self.run_job(), ("completed", "8"))
        self.assertEqual(len(self.sent), 1)

    def test_native_flight_unavailable_without_unit_never_invents_completion(self):
        self.mode = "flight-missing"
        self.assertEqual(self.run_job(), ("unknown", None))
        self.receipt()
        self.assertEqual(self.run_job(), ("unknown", None))
        attempt = self.runner.journal.get("workload-1", self.deployment)["attempts"][0]
        self.assertEqual((attempt["rpc_status"], attempt["error_code"]), ("unknown", "unknown"))
        self.assertEqual(len(self.sent), 1)

    def test_disarm_after_prepare_prevents_native_dispatch(self):
        def disarmed():
            raise Denial("disarmed")
        with self.assertRaises(Denial):
            self.run_job(authorize=disarmed)
        self.assertEqual(self.sent, [])
        self.assertFalse(self.runner.journal.get("workload-1", self.deployment)["attempts"][0]["sent"])

    def test_unknown_receipt_health_denies_before_prepare_or_send(self):
        self.snapshot["receipt_observation_known"] = None
        with self.assertRaisesRegex(Denial, "disconnected"):
            self.run_job()
        self.assertEqual(self.prepare_count, 0)
        self.assertEqual(self.sent, [])
        self.assertFalse(self.runner.journal.get("workload-1", self.deployment)["attempts"][0]["sent"])

    def test_stopping_prevents_remaining_requests_and_preserves_partial_outcomes(self):
        self.packet["action"]["requests"] = 2
        self.assertEqual(self.run_job(stopping=lambda: bool(self.sent)), ("cancelled", None))
        attempts = self.runner.journal.get("workload-1", self.deployment)["attempts"]
        self.assertTrue(attempts[0]["sent"])
        self.assertFalse(attempts[1]["sent"])
        self.assertEqual(attempts[1]["rpc_status"], "cancelled")
        self.assertEqual(len(self.sent), 1)

    def test_server_owned_presets_are_closed_and_bounds_are_not_boolean_coercions(self):
        action = self.packet["action"]
        for change in ({"profile": "arbitrary"}, {"requests": True}, {"requests": 9}, {"duration_ms": 10001},
                       {"schema": "untrusted"}, {"profile": "elephant", "requests": 3}, {"streams": True},
                       {"streams": 0}, {"streams": 2}, {"streams": 5}):
            with self.subTest(change=change), self.assertRaises(Denial):
                preset_for({**action, **change})
        preset = preset_for(action)
        preset["rows_per_request"] = 999
        self.assertEqual(PRESETS[0]["rows_per_request"], 3)

    def test_stale_wrong_deployment_or_mismatched_receipt_never_completes(self):
        self.run_job()
        self.receipt()
        self.snapshot["receipts"][0]["digest"] = "b" * 64
        self.assertEqual(self.runner.outcome("workload-1", self.deployment), ("unknown", None))
        self.receipt()
        self.snapshot["freshness"]["age_ns"] = "5000000001"
        self.snapshot["freshness"]["status"] = "stale"
        with self.assertRaises(Denial):
            self.runner.outcome("workload-1", self.deployment)
        with self.assertRaises(Denial):
            self.runner.journal.get("workload-1", {**self.deployment, "incarnation": "foreign"})

    def test_marker_ranges_and_attempt_ids_are_stable_disjoint_and_bounded(self):
        self.ledger.reserve(self.packet)
        first, fresh = self.runner.journal.create("workload-1", self.deployment)
        self.assertTrue(fresh)
        self.assertEqual(self.runner.journal.create("workload-1", self.deployment), (first, False))
        other = copy.deepcopy(self.packet)
        other["operation_id"] = "workload-2"
        other["action"].update(profile="elephant", requests=2)
        self.ledger.reserve(other)
        second, _ = self.runner.journal.create("workload-2", self.deployment)
        a = first["attempts"][0]
        b, c = second["attempts"]
        self.assertLess(a["first"] + a["rows"], b["first"])
        self.assertLessEqual(b["first"] + b["rows"], c["first"])
        self.assertLess(c["first"] + c["rows"], 2**63)
        self.assertNotEqual(a["attempt_id"], b["attempt_id"])

    def native_fixture(self, nullable=False):
        import pyarrow as pa
        root = Path(self.directory.name)
        schema = pa.schema([pa.field("id", pa.int64(), nullable=nullable)])
        with pa.OSFile(str(root / "schema-0.arrow"), "wb") as stream, pa.ipc.new_stream(stream, schema):
            pass
        name, identity = "publication-gate-synthetic-workload-gateway", "a" * 64
        record = {"id": identity, "started_at": "synthetic-start", "image": "sha256:" + "b" * 64}
        state = {
            "Id": identity, "Name": "/" + name, "Image": record["image"],
            "Config": {"Labels": {"delta-runtime": "publication-gate-synthetic-workload"}},
            "State": {"StartedAt": record["started_at"], "Running": True, "Paused": False},
            "NetworkSettings": {"Ports": {"50051/tcp": [{"HostIp": "127.0.0.1", "HostPort": "18500"}]}},
        }
        runtime = SimpleNamespace(
            root=root, project="publication-gate-synthetic-workload", names={"gateway": name},
            records={name: record}, engine=SimpleNamespace(get=lambda _: copy.deepcopy(state)),
            owned_container=lambda _: identity,
            config={"tables": [{"key": "main", "path": ["telemetry", "runtime", "main"],
                                "fingerprints": ["synthetic-fingerprint"]}]},
        )
        return NativeWorkload(runtime), state

    def test_native_preflight_preserves_nonnullable_baseline_and_exact_fingerprint(self):
        native, _ = self.native_fixture()
        result = SimpleNamespace(returncode=0, stdout=b"synthetic-fingerprint\n")
        with patch("live_workload.subprocess.run", return_value=result) as fingerprint:
            native.prepare()
        self.assertEqual(native.port, 18500)
        self.assertFalse(native.schema.field("id").nullable)
        self.assertEqual(fingerprint.call_args.kwargs["timeout"], 3)
        with patch("live_workload.subprocess.run", return_value=SimpleNamespace(returncode=0, stdout=b"wrong")):
            with self.assertRaises(Denial):
                native.prepare()

    def test_native_nullable_schema_or_foreign_port_fails_without_network_effects(self):
        native, _ = self.native_fixture(nullable=True)
        with patch("live_workload.subprocess.run") as fingerprint:
            with self.assertRaises(Denial):
                native.prepare()
        fingerprint.assert_not_called()
        native, state = self.native_fixture()
        state["NetworkSettings"]["Ports"]["50051/tcp"][0]["HostIp"] = "0.0.0.0"
        with patch("live_workload.subprocess.run") as fingerprint:
            with self.assertRaises(Denial):
                native.prepare()
        fingerprint.assert_not_called()


if __name__ == "__main__":
    unittest.main()
