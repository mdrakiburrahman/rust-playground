"""Synthetic shared-dispatch fixtures; not guarded native HTTP qualification."""
import copy
from pathlib import Path
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_contract import Denial, DeploymentFence, SessionFence
from live_effects import EffectExecutor
from live_faults import boot_identity
from live_ledger import OperationLedger


class SharedEffectTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.ledger = OperationLedger(Path(self.directory.name) / "operations.sqlite")
        self.deployment = {"id": "poc", "incarnation": "deployment-1"}
        self.resource = {"id": "resource-collector1", "incarnation": "container-1"}
        self.writer = {"collector": "collector1", "process": "writer-1", "generation": "generation-1"}
        self.target = {"resource": self.resource, "writer": self.writer, "policy_revision": "0"}
        now = time.monotonic_ns()
        self.fence = DeploymentFence(self.deployment, "7", now, True, True,
                                     frozenset({"admission", "exclusion", "fault", "workload"}),
                                     {self.resource["id"]: self.target})
        self.session = SessionFence("http://127.0.0.1:18765", True, True, "live",
                                    self.deployment, "selection-1", "arm-1", now + 20_000_000_000)
        self.packet = {
            "kind": "command", "schema_version": 1, "operation_id": "fault-1",
            "selection_id": "selection-1", "arm_id": "arm-1", "deployment": self.deployment, "revision": "7",
            "action": {"operation": "fault", "resource": self.resource, "fault": "pause", "duration_ms": 500},
        }
        self.records = {}
        self.fault_calls, self.workload_calls = 0, 0
        self.workload_status = ("unknown", None)
        self.age = "0"
        self.status = "active"
        self.faults = SimpleNamespace(
            runtime=None, journal=SimpleNamespace(get=lambda operation_id: copy.deepcopy(self.records[operation_id])),
            launch=self.launch)
        self.workload = SimpleNamespace(run=self.run_workload, outcome=lambda *_: self.workload_status)
        self.executor = EffectExecutor(self.ledger, lambda _: {}, lambda _: self.resource, self.snapshot,
                                       self.faults, self.workload)

    def tearDown(self):
        self.executor.close()
        self.ledger.close()
        self.directory.cleanup()

    def test_capacity_scope_advertises_and_admits_only_one_request(self):
        executor = EffectExecutor(self.ledger, lambda _: {}, lambda _: self.resource, self.snapshot,
                                  workload=self.workload, capacity=SimpleNamespace(),
                                  policy_enabled=False, profiles={"small"})
        try:
            self.assertEqual(executor.maximums["workload"], 1)
            self.assertEqual([preset["maximum_requests"] for preset in executor.presets], [1])
            action = {"operation": "workload", "profile": "small", "requests": 1,
                      "duration_ms": 10000, "streams": 1}
            executor.validate_action(action)
            with self.assertRaisesRegex(Denial, "unsupported"):
                executor.validate_action({**action, "requests": 2})
            self.assertEqual(self.workload_calls, 0)
        finally:
            executor.close()

    def snapshot(self):
        return {
            "kind": "snapshot", "schema_version": 1,
            "bridge": {"id": "bridge", "incarnation": "bridge-1"}, "deployment": self.deployment,
            "revision": "8", "freshness": {"status": "fresh", "age_ns": self.age, "maximum_age_ns": "5000000000"},
            "replicas": [{
                "resource": self.resource, "writer": self.writer, "role": "collector", "status": self.status,
                "policy_revision": "0", "applied_policy_revision": "0",
                "excluded": False, "block_streams": False, "retiring": False,
                "identity_verified": True, "running": True, "paused": False,
            }], "tables": [], "receipts": [], "metrics": [],
            "coverage": {"complete": False, "missing": ["cpu"], "dropped": 0},
            "receipt_observation_known": True,
        }

    def launch(self, operation_id, action, authorize):
        authorize()
        self.fault_calls += 1
        self.records[operation_id] = {
            "intent": {"deployment": self.deployment, "action": copy.deepcopy(action)},
            "phase": "paused", "applied": True, "error_code": None,
            "repair_boot": None, "repaired_ns": None,
        }

    def run_workload(self, operation_id, deployment, authorize, admitted, stopping):
        authorize()
        admitted()
        self.workload_calls += 1

    def apply(self, packet=None):
        return self.executor.apply(packet or self.packet, self.fence, self.session, self.session.allowed_origin)

    def repaired(self):
        self.records["fault-1"].update(phase="repaired", repair_boot=boot_identity(),
                                       repaired_ns=time.monotonic_ns() - 1_000_000)

    def test_fault_stays_pending_until_exact_repair_and_causally_later_snapshot(self):
        self.assertEqual(self.apply()["status"], "pending")
        self.repaired()
        self.age = "1000000000"
        with patch("live_effects.exact_state", return_value={"Running": True, "Paused": False}):
            self.assertEqual(self.executor.reconcile("fault-1", self.deployment)["status"], "pending")
            self.age = "0"
            done = self.executor.reconcile("fault-1", self.deployment)
        self.assertEqual((done["status"], done["applied_revision"]), ("completed", "8"))
        self.assertEqual(self.apply(), done)
        self.assertEqual(self.fault_calls, 1)

    def test_unknown_application_or_failed_repair_never_completes(self):
        self.apply()
        self.repaired()
        self.records["fault-1"]["applied"] = False
        self.assertEqual(self.executor.reconcile("fault-1", self.deployment)["status"], "unknown")
        self.records["fault-1"].update(applied=True, phase="repair-required")
        self.assertEqual(self.executor.reconcile("fault-1", self.deployment)["status"], "unknown")
        self.assertEqual(self.fault_calls, 1)

    def test_wrong_boot_paused_or_nonready_resource_never_completes(self):
        self.apply()
        self.repaired()
        self.records["fault-1"]["repair_boot"] = "different-boot"
        with patch("live_effects.exact_state", return_value={"Running": True, "Paused": False}):
            self.assertEqual(self.executor.reconcile("fault-1", self.deployment)["status"], "pending")
        self.records["fault-1"]["repair_boot"] = boot_identity()
        with patch("live_effects.exact_state", return_value={"Running": True, "Paused": True}):
            self.assertEqual(self.executor.reconcile("fault-1", self.deployment)["status"], "unknown")
        self.status = "recovering"
        with patch("live_effects.exact_state", return_value={"Running": True, "Paused": False}):
            self.assertEqual(self.executor.reconcile("fault-1", self.deployment)["status"], "unknown")

    def test_native_final_handshake_uses_live_recheck_and_denial_prevents_fault(self):
        def disarmed():
            raise Denial("disarmed")
        result = self.executor._reserve(self.packet, self.fence, self.session, self.session.allowed_origin)
        self.assertIsNone(result)
        rejected = self.executor._apply_reserved(
            self.packet, self.fence, self.session, self.session.allowed_origin, disarmed)
        self.assertEqual((rejected["status"], rejected["error_code"]), ("rejected", "disarmed"))
        self.assertEqual(self.fault_calls, 0)

    def test_single_admission_slot_is_shared_and_same_id_can_read_while_busy(self):
        entered, release = threading.Event(), threading.Event()
        original = self.faults.launch
        def held(*args):
            entered.set()
            release.wait(timeout=3)
            return original(*args)
        self.faults.launch = held
        try:
            self.executor.submit(self.packet, self.fence, self.session, self.session.allowed_origin)
            self.assertTrue(entered.wait(timeout=2))
            self.assertEqual(self.executor.submit(
                self.packet, self.fence, self.session, self.session.allowed_origin)["status"], "requested")
            other = copy.deepcopy(self.packet)
            other.update(operation_id="workload-1", action={
                "operation": "workload", "profile": "small", "requests": 1, "duration_ms": 1000, "streams": 1})
            with self.assertRaises(Denial):
                self.executor.submit(other, self.fence, self.session, self.session.allowed_origin)
            with self.assertRaises(Denial):
                self.ledger.get("workload-1", self.deployment)
        finally:
            release.set()

    def test_workload_observation_not_dispatch_advances_completion(self):
        self.packet.update(operation_id="workload-1", action={
            "operation": "workload", "profile": "small", "requests": 1, "duration_ms": 1000, "streams": 1})
        self.assertEqual(self.apply()["status"], "unknown")
        self.workload_status = ("completed", "8")
        done = self.executor.reconcile("workload-1", self.deployment)
        self.assertEqual((done["status"], done["applied_revision"]), ("completed", "8"))
        self.assertEqual(self.apply(), done)
        self.assertEqual(self.workload_calls, 1)

    def test_bounds_and_unqualified_adapters_fail_before_reservation(self):
        self.packet["action"]["duration_ms"] = 3001
        with self.assertRaises(Denial):
            self.apply()
        with self.assertRaises(Denial):
            self.ledger.get("fault-1", self.deployment)
        self.packet["action"]["duration_ms"] = 500
        self.executor.operations = frozenset({"exclusion", "admission"})
        with self.assertRaises(Denial):
            self.apply()
        self.assertEqual(self.fault_calls, 0)


if __name__ == "__main__":
    unittest.main()
