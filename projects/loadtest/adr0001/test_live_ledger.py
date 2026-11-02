import copy
import json
from pathlib import Path
import tempfile
import unittest

from live_contract import Denial
from live_ledger import OperationLedger


class OperationLedgerTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "operations.sqlite"
        self.deployment = {"id": "poc", "incarnation": "deployment-1"}
        self.command = {
            "kind": "command", "schema_version": 1, "operation_id": "operation-1",
            "selection_id": "selection-1", "arm_id": "arm-1", "deployment": self.deployment,
            "revision": "7", "action": {"operation": "replicas", "role": "collector", "desired": 3},
            "bridge": {"id": "bridge", "incarnation": "bridge-1"}, "safety_revision": "3",
        }
        self.ledger = OperationLedger(self.path)

    def tearDown(self):
        self.ledger.close()
        self.directory.cleanup()

    def test_exact_intent_survives_restart_without_session_secrets_or_effect_replay(self):
        first, fresh = self.ledger.reserve(self.command)
        self.assertTrue(fresh)
        self.ledger.close()
        self.ledger = OperationLedger(self.path)
        retried = copy.deepcopy(self.command)
        retried.update(arm_id="new-arm", selection_id="new-selection")
        repeated, fresh = self.ledger.reserve(retried)
        self.assertFalse(fresh)
        self.assertEqual(first, repeated)
        self.assertEqual(self.ledger.unresolved(), [first])
        content = self.path.read_bytes()
        for secret in (b"arm-1", b"selection-1", b"new-arm"):
            self.assertNotIn(secret, content)

    def test_id_collision_never_changes_original_intent(self):
        first, _ = self.ledger.reserve(self.command)
        for field in ("action", "revision", "deployment"):
            conflict = copy.deepcopy(self.command)
            if field == "action":
                conflict[field]["desired"] = 4
            elif field == "revision":
                conflict[field] = "8"
            else:
                conflict[field]["incarnation"] = "deployment-2"
            with self.subTest(field=field), self.assertRaisesRegex(Denial, "conflict"):
                self.ledger.reserve(conflict)
        self.assertEqual(self.ledger.get("operation-1", self.deployment), first)

    def test_lost_response_reconciliation_preserves_requested_and_applied_revisions(self):
        self.ledger.reserve(self.command)
        self.ledger.advance("operation-1", self.deployment, "pending")
        self.ledger.advance("operation-1", self.deployment, "applied", "8")
        done = self.ledger.advance("operation-1", self.deployment, "completed")
        self.assertEqual(done["requested_revision"], "7")
        self.assertEqual(done["applied_revision"], "8")
        self.assertEqual(self.ledger.unresolved(), [])
        self.assertEqual(self.ledger.advance("operation-1", self.deployment, "completed"), done)
        with self.assertRaisesRegex(Denial, "conflict"):
            self.ledger.advance("operation-1", self.deployment, "pending")

    def test_unknown_timeout_and_missing_operation_are_not_success(self):
        self.ledger.reserve(self.command)
        self.ledger.advance("operation-1", self.deployment, "unknown", error_code="unknown")
        self.assertEqual(len(self.ledger.unresolved()), 1)
        timed_out = self.ledger.advance("operation-1", self.deployment, "timeout", error_code="timeout")
        self.assertIsNone(timed_out["applied_revision"])
        self.assertEqual(len(self.ledger.unresolved()), 1)
        eventual = self.ledger.advance("operation-1", self.deployment, "completed", "8")
        self.assertEqual(eventual["status"], "completed")
        with self.assertRaisesRegex(Denial, "unknown-operation"):
            self.ledger.get("missing", self.deployment)
        with self.assertRaisesRegex(Denial, "wrong-target"):
            self.ledger.get("operation-1", {"id": "poc", "incarnation": "other"})

    def test_capacity_is_bounded_and_existing_receipt_still_reconciles(self):
        self.ledger.close()
        self.ledger = OperationLedger(self.path, limit=1)
        self.ledger.reserve(self.command)
        extra = copy.deepcopy(self.command)
        extra["operation_id"] = "operation-2"
        with self.assertRaisesRegex(Denial, "limit"):
            self.ledger.reserve(extra)
        _, fresh = self.ledger.reserve(self.command)
        self.assertFalse(fresh)

    def test_new_unknown_observation_updates_reason_without_rewriting_applied_identity(self):
        self.ledger.reserve(self.command)
        self.ledger.advance("operation-1", self.deployment, "pending")
        self.ledger.advance("operation-1", self.deployment, "applied", "8")
        self.ledger.advance("operation-1", self.deployment, "unknown", error_code="disconnected")
        current = self.ledger.advance("operation-1", self.deployment, "unknown", error_code="stale")
        self.assertEqual((current["status"], current["error_code"]), ("unknown", "stale"))
        self.assertEqual((current["requested_revision"], current["applied_revision"]), ("7", "8"))
        with self.assertRaisesRegex(Denial, "conflict"):
            self.ledger.advance("operation-1", self.deployment, "unknown", "9", error_code="unknown")
        self.ledger.advance("operation-1", self.deployment, "completed", "10")
        with self.assertRaisesRegex(Denial, "conflict"):
            self.ledger.advance("operation-1", self.deployment, "completed", error_code="unknown")

    def test_cannot_invent_completion_without_applied_revision(self):
        self.ledger.reserve(self.command)
        self.ledger.advance("operation-1", self.deployment, "pending")
        with self.assertRaisesRegex(Denial, "invalid"):
            self.ledger.advance("operation-1", self.deployment, "completed")

    def test_snapshot_revision_survives_reopen_and_rejects_competing_publishers(self):
        self.assertEqual(self.ledger.bind_deployment(self.deployment), 0)
        self.ledger.publish_revision(self.deployment, 0, 1)
        self.ledger.close()
        self.ledger = OperationLedger(self.path)
        self.assertEqual(self.ledger.bind_deployment(self.deployment), 1)
        with self.assertRaisesRegex(Denial, "conflict"):
            self.ledger.publish_revision(self.deployment, 0, 1)
        with self.assertRaisesRegex(Denial, "wrong-target"):
            self.ledger.bind_deployment({**self.deployment, "incarnation": "replacement"})
        self.ledger.publish_revision(self.deployment, 1, 2)

    def test_existing_operations_seed_revision_watermark_without_replaying(self):
        self.ledger.reserve(self.command)
        self.ledger.advance("operation-1", self.deployment, "pending")
        self.ledger.advance("operation-1", self.deployment, "applied", "9")
        self.assertEqual(self.ledger.bind_deployment(self.deployment), 9)
        self.ledger.publish_revision(self.deployment, 9, 10)
        self.assertEqual(self.ledger.get("operation-1", self.deployment)["status"], "applied")

    def test_safety_watermark_survives_restart_and_applied_observation_is_separate(self):
        self.ledger.bind_deployment(self.deployment)
        safety = self.ledger.next_safety_revision()
        self.ledger.publish_revision(self.deployment, 0, 1, safety)
        self.ledger.reserve(self.command)
        self.ledger.advance("operation-1", self.deployment, "pending")
        done = self.ledger.advance("operation-1", self.deployment, "completed", "1")
        self.assertEqual(done["requested_safety_revision"], "3")
        self.assertEqual(done["applied_safety_revision"], str(safety))
        self.ledger.close()
        self.ledger = OperationLedger(self.path)
        self.assertGreater(self.ledger.next_safety_revision(), safety)
        self.assertEqual(self.ledger.get("operation-1", self.deployment), done)

    def test_bridge_boot_and_safety_revision_are_part_of_same_id_immutable_intent(self):
        self.ledger.reserve(self.command)
        for field in ("safety_revision", "bridge"):
            changed = copy.deepcopy(self.command)
            if field == "bridge":
                changed[field]["incarnation"] = "new-boot"
            else:
                changed[field] = "4"
            with self.subTest(field=field), self.assertRaisesRegex(Denial, "conflict"):
                self.ledger.reserve(changed)

    def test_old_ledger_results_are_explicitly_unknown_safety_not_reauthorized(self):
        self.ledger.reserve(self.command)
        old = self.ledger.get("operation-1", self.deployment)
        del old["requested_safety_revision"], old["applied_safety_revision"]
        self.ledger.connection.execute("UPDATE operations SET result=?", (json.dumps(old),))
        self.ledger.connection.execute("PRAGMA user_version=0")
        self.ledger.close()
        self.ledger = OperationLedger(self.path)
        migrated = self.ledger.get("operation-1", self.deployment)
        self.assertIsNone(migrated["requested_safety_revision"])
        self.assertIsNone(migrated["applied_safety_revision"])
        self.assertEqual(migrated["status"], "requested")

    def test_feed_and_safety_observation_mapping_are_bounded_and_atomic(self):
        self.ledger.bind_deployment(self.deployment)
        safety = self.ledger.next_safety_revision()
        for revision in range(1, 70):
            self.ledger.publish_revision(self.deployment, revision - 1, revision, safety)
        self.assertEqual(self.ledger.connection.execute(
            "SELECT COUNT(*) FROM bridge_observations").fetchone()[0], 64)
        with self.assertRaisesRegex(Denial, "conflict"):
            self.ledger.publish_revision(self.deployment, 68, 69, safety)
        self.assertEqual(self.ledger.bind_deployment(self.deployment), 69)


if __name__ == "__main__":
    unittest.main()
