"""Synthetic policy adapters only; not real SQL/Delta or HTTP effect acceptance."""
import copy
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from live_contract import Denial, DeploymentFence, SessionFence
from live_executor import PolicyExecutor
from live_ledger import OperationLedger


class PolicyExecutorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "operations.sqlite"
        self.ledger = OperationLedger(self.path)
        self.deployment = {"id": "poc", "incarnation": "deployment-1"}
        self.writer = {"collector": "collector1", "process": "writer-1", "generation": "generation-1"}
        self.target = {"resource": {"id": "resource-collector1", "incarnation": "container-1"},
                       "writer": self.writer, "policy_revision": "0"}
        self.command = {
            "kind": "command", "schema_version": 1, "operation_id": "operation-1",
            "selection_id": "selection-1", "arm_id": "arm-1", "deployment": self.deployment,
            "revision": "7", "action": {"operation": "admission", "target": self.target, "enabled": True},
        }
        now = time.monotonic_ns()
        self.fence = DeploymentFence(
            self.deployment, "7", now, True, True, frozenset({"admission", "exclusion"}),
            {self.target["resource"]["id"]: self.target},
        )
        self.session = SessionFence("http://127.0.0.1:18765", True, True, "live",
                                    self.deployment, "selection-1", "arm-1", now + 20_000_000_000)
        self.current_resource = copy.deepcopy(self.target["resource"])
        self.state = {"members": [{"writer": self.writer}], "policies": {}}
        self.policy_calls = []
        self.lose_response = False
        self.executor = PolicyExecutor(self.ledger, self.post, lambda _: self.current_resource, self.snapshot)

    def tearDown(self):
        self.executor.close()
        self.ledger.close()
        self.directory.cleanup()

    def post(self, operation):
        if operation["op"] == "view":
            return copy.deepcopy(self.state)
        self.policy_calls.append(copy.deepcopy(operation))
        self.state["policies"]["collector1"] = {
            "revision": operation["revision"] + 1, "incarnation": self.writer,
            "operation": operation["operation"], "excluded": operation["excluded"],
            "block_streams": operation["block_streams"], "retiring": operation["retiring"],
            "approved": False, "applied_revision": 0,
        }
        if self.lose_response:
            raise TimeoutError("synthetic lost response")
        return {}

    def snapshot(self):
        policy = self.state["policies"]["collector1"]
        return {
            "kind": "snapshot", "schema_version": 1,
            "bridge": {"id": "bridge", "incarnation": "bridge-1"}, "deployment": self.deployment,
            "revision": "8", "freshness": {"status": "fresh", "age_ns": "0", "maximum_age_ns": "5000000000"},
            "replicas": [{
                "resource": self.current_resource, "writer": self.writer, "role": "collector", "status": "active",
                "policy_revision": str(policy["revision"]), "applied_policy_revision": str(policy["applied_revision"]),
                "excluded": policy["excluded"], "block_streams": policy["block_streams"], "retiring": policy["retiring"],
            }],
            "tables": [], "receipts": [], "metrics": [],
            "coverage": {"complete": False, "missing": ["receipts"], "dropped": 0},
        }

    def apply(self, command=None):
        return self.executor.apply(command or self.command, self.fence, self.session, self.session.allowed_origin)

    def test_policy_ack_is_separate_from_request_and_native_commit(self):
        result = self.apply()
        self.assertEqual(result["status"], "applied")
        self.assertEqual((result["requested_revision"], result["applied_revision"]), ("7", "8"))
        self.assertTrue(self.policy_calls[0]["block_streams"])
        self.assertFalse(self.policy_calls[0]["excluded"])
        self.assertFalse(self.policy_calls[0]["retiring"])
        self.state["policies"]["collector1"]["applied_revision"] = 1
        done = self.executor.reconcile("operation-1", self.deployment)
        self.assertEqual(done["status"], "completed")
        self.assertEqual(self.apply(), done)
        self.assertEqual(len(self.policy_calls), 1)

    def test_lost_response_and_ledger_reopen_never_replay_effect(self):
        self.lose_response = True
        self.assertEqual(self.apply()["status"], "unknown")
        self.executor.close()
        self.ledger.close()
        self.ledger = OperationLedger(self.path)
        self.executor = PolicyExecutor(self.ledger, self.post, lambda _: self.current_resource, self.snapshot)
        self.state["policies"]["collector1"]["applied_revision"] = 1
        done = self.executor.reconcile("operation-1", self.deployment)
        self.assertEqual(done["status"], "completed")
        self.assertEqual(len(self.policy_calls), 1)
        intent = self.ledger.intent("operation-1", self.deployment)
        self.assertEqual(set(intent), {"deployment", "revision", "action"})
        self.assertNotIn(b"arm-1", self.path.read_bytes())

    def test_changed_resource_is_rejected_before_effect(self):
        self.current_resource["incarnation"] = "replacement"
        result = self.apply()
        self.assertEqual((result["status"], result["error_code"]), ("rejected", "wrong-target"))
        self.assertEqual(self.policy_calls, [])

    def test_replacement_after_effect_stays_unknown_not_replayed_or_completed(self):
        self.apply()
        self.current_resource["incarnation"] = "replacement"
        result = self.executor.reconcile("operation-1", self.deployment)
        self.assertEqual((result["status"], result["error_code"]), ("unknown", "wrong-target"))
        self.assertEqual(len(self.policy_calls), 1)

    def test_same_id_different_action_conflicts_without_second_effect(self):
        self.apply()
        conflict = copy.deepcopy(self.command)
        conflict["action"]["enabled"] = False
        with self.assertRaisesRegex(Denial, "conflict"):
            self.apply(conflict)
        self.assertEqual(len(self.policy_calls), 1)

    def test_unqualified_authority_cannot_reserve_or_execute(self):
        self.fence = DeploymentFence(self.deployment, "7", connected=True)
        with self.assertRaisesRegex(Denial, "unqualified"):
            self.apply()
        self.assertEqual(self.ledger.unresolved(), [])
        self.assertEqual(self.policy_calls, [])

    def test_missing_observer_confirmation_does_not_invent_applied_revision(self):
        original = self.executor.snapshot
        def stale():
            snapshot = original()
            snapshot["replicas"][0]["policy_revision"] = "0"
            return snapshot
        self.executor.snapshot = stale
        result = self.apply()
        self.assertEqual(result["status"], "unknown")
        self.assertIsNone(result["applied_revision"])
        self.assertEqual(len(self.policy_calls), 1)

    def test_expired_arm_after_preflight_cannot_start_a_policy_effect(self):
        with patch("live_executor.authorize_command", side_effect=[self.command, Denial("disarmed")]) as guard:
            result = self.apply()
        self.assertEqual(guard.call_count, 2)
        self.assertEqual((result["status"], result["error_code"]), ("rejected", "disarmed"))
        self.assertEqual(self.policy_calls, [])

    def test_async_submission_has_one_slot_and_never_queues_extra_effects(self):
        entered, release = threading.Event(), threading.Event()
        original = self.post
        def held(operation):
            if operation["op"] == "view" and not entered.is_set():
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("synthetic blocked preflight")
            return original(operation)
        self.executor.post = held
        result = self.executor.submit(self.command, self.fence, self.session, self.session.allowed_origin)
        self.assertIn(result["status"], ("requested", "pending"))
        self.assertTrue(entered.wait(2))
        extra = copy.deepcopy(self.command)
        extra["operation_id"] = "operation-2"
        try:
            replay = self.executor.submit(self.command, self.fence, self.session, self.session.allowed_origin)
            self.assertEqual(replay["operation_id"], self.command["operation_id"])
            different = copy.deepcopy(self.command)
            different["action"]["enabled"] = False
            with self.assertRaisesRegex(Denial, "conflict"):
                self.apply(different)
            with self.assertRaisesRegex(Denial, "limit"):
                self.apply(extra)
        finally:
            release.set()
        self.assertTrue(self.executor.lock.acquire(timeout=2))
        self.executor.lock.release()
        self.assertEqual(len(self.policy_calls), 1)
        with self.assertRaisesRegex(Denial, "unknown-operation"):
            self.ledger.get("operation-2", self.deployment)

    def test_unexpected_worker_failure_is_visible_unknown_and_disables_new_effects(self):
        def failed(_):
            raise AssertionError("private-synthetic-diagnostic")
        self.executor.post = failed
        with patch("builtins.print") as output:
            self.executor.submit(self.command, self.fence, self.session, self.session.allowed_origin)
            self.executor.pool.shutdown(wait=True)
            result = self.ledger.get("operation-1", self.deployment)
        self.assertTrue(self.executor.failed.is_set())
        self.assertEqual((result["status"], result["error_code"]), ("unknown", "failed"))
        self.assertNotIn("private-synthetic-diagnostic", str(output.call_args_list))
        with self.assertRaisesRegex(Denial, "disconnected"):
            self.apply()

    def test_native_revision_bound_denies_before_durable_reservation(self):
        self.target["policy_revision"] = str(2**63 - 1)
        with self.assertRaisesRegex(Denial, "limit"):
            self.apply()
        self.assertEqual(self.ledger.unresolved(), [])


if __name__ == "__main__":
    unittest.main()
