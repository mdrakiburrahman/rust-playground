"""Synthetic SQL/physical fixtures; real Delta acceptance is separate."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from live_receipts import ReceiptObserver, proposal_digest, sql_projection


class LiveReceiptTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.runtime = SimpleNamespace(root=self.root, config={"tables": [{"key": "main"}]},
                                       sql_name="owned-sql", owned_container=lambda _: "a" * 64)
        self.observation = {"state": {"tables": {}}, "receipts": []}
        self.history = self.table("main", 2, data=True)
        self.reader = Mock(side_effect=lambda _, version: {"data-1.parquet"} if version else set())
        self.observer = ReceiptObserver(self.runtime, lambda: copy.deepcopy(self.observation), self.reader)

    def tearDown(self):
        self.directory.cleanup()

    def table(self, name, count, data=False):
        root = self.root / "delta-private/tables" / name
        (root / "_delta_log").mkdir(parents=True, exist_ok=True)
        history = []
        for version in range(count):
            references = []
            if data and version:
                content = b"synthetic immutable parquet fixture"
                filename = f"data-{version}.parquet"
                (root / filename).write_bytes(content)
                references = [{"name": filename, "bytes": len(content),
                               "sha256": hashlib.sha256(content).hexdigest()}]
            proposal = {
                "table": name, "operation": f"unit-{name}-{version}", "identity": {
                    "controller": "controller-test", "controller_generation": 1, "membership": "membership-test",
                    "process": "writer-test", "local_generation": 1, "epoch": 1},
                "predecessor": version - 1, "version": version,
                "actions": '{"metaData":{"id":"synthetic-fixture"}}\n', "data": references,
            }
            (root / "_delta_log" / f"{version:020}.json").write_text(proposal["actions"])
            history.append(proposal)
            self.observation["receipts"].append({
                "table_id": name, "operation_id": proposal["operation"],
                "version": version, "digest": proposal_digest(proposal),
            })
        self.observation["state"]["tables"][name] = {"decisions": history}
        return history

    def test_completion_requires_sql_receipt_physical_digests_and_ordinary_reader(self):
        receipts, missing, dropped = self.observer.sample()
        self.assertEqual([item["completion"] for item in receipts], ["completed", "completed"])
        self.assertEqual((missing, dropped), ([], 0))
        self.reader.assert_called_once_with(self.root / "delta-private/tables/main", 1)
        self.observer.sample()
        self.assertEqual(self.reader.call_count, 1)
        self.assertEqual(receipts[1]["operation_id"], self.history[1]["operation"])

    def test_acceptance_or_visible_bytes_without_final_sql_receipt_is_not_completion(self):
        self.observation["receipts"].pop()
        receipts, missing, dropped = self.observer.sample()
        self.assertEqual([item["completion"] for item in receipts], ["completed", "accepted"])
        self.assertEqual((missing, dropped), ([], 0))
        self.reader.assert_called_once_with(self.root / "delta-private/tables/main", 0)

    def test_missing_or_changed_log_and_data_remain_unknown_not_completed(self):
        self.observer.sample()
        target = self.root / "delta-private/tables/main/data-1.parquet"
        target.write_bytes(b"x" * target.stat().st_size)
        receipts, missing, dropped = self.observer.sample()
        self.assertTrue(all(item["completion"] == "unknown" for item in receipts))
        self.assertIn("receipt-physical-unverified", missing)
        self.assertGreater(dropped, 0)
        self.reader.assert_called_once()

    def test_symlink_log_never_becomes_physical_completion(self):
        log = self.root / "delta-private/tables/main/_delta_log/00000000000000000001.json"
        target = self.root / "displaced-log.json"
        log.rename(target)
        log.symlink_to(target)
        receipts, missing, _ = self.observer.sample()
        self.assertTrue(all(item["completion"] == "unknown" for item in receipts))
        self.assertIn("receipt-physical-unverified", missing)

    def test_reader_failure_or_file_set_mismatch_remains_explicitly_unknown(self):
        for result in (set(), RuntimeError("private-reader-diagnostic")):
            with self.subTest(result=type(result).__name__):
                self.reader.side_effect = result if isinstance(result, Exception) else lambda *_: result
                receipts, missing, dropped = self.observer.sample()
                self.assertTrue(all(item["completion"] == "unknown" for item in receipts))
                self.assertIn("receipt-reader-unavailable", missing)
                self.assertEqual(dropped, 1)

    def test_sql_digest_collision_and_orphan_receipt_are_not_success(self):
        self.observation["receipts"][1]["digest"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "exact accepted decision"):
            self.observer.sample()
        self.observation["receipts"][1]["digest"] = proposal_digest(self.history[1])
        extra = copy.deepcopy(self.observation["receipts"][1])
        extra["operation_id"] = "unaccepted-unit"
        self.observation["receipts"].append(extra)
        with self.assertRaisesRegex(ValueError, "no accepted decision"):
            self.observer.sample()

    def test_missing_earlier_final_receipt_cannot_be_skipped(self):
        self.observation["receipts"].pop(0)
        receipts, missing, _ = self.observer.sample()
        self.assertEqual([item["completion"] for item in receipts], ["accepted", "unknown"])
        self.assertIn("receipt-physical-unverified", missing)
        self.reader.assert_not_called()

    def test_verification_budget_does_not_fabricate_complete_coverage(self):
        with patch("live_receipts.MAX_VERIFY_BYTES", 1):
            receipts, missing, dropped = self.observer.sample()
        self.assertTrue(all(item["completion"] == "unknown" for item in receipts))
        self.assertIn("receipt-physical-unverified", missing)
        self.assertEqual(dropped, 2)

    def test_history_projection_is_bounded_balanced_and_reports_omitted_records(self):
        self.observation = {"state": {"tables": {}}, "receipts": []}
        self.runtime.config["tables"] = [{"key": name} for name in ("main", "dictionary", "cas")]
        for name in ("main", "dictionary", "cas"):
            self.table(name, 45)
        self.reader.side_effect = lambda *_: set()
        receipts, missing, dropped = self.observer.sample()
        self.assertEqual(len(receipts), 128)
        self.assertEqual({item["table_id"] for item in receipts}, {"main", "dictionary", "cas"})
        self.assertEqual(dropped, 7)
        self.assertIn("receipt-history-truncated", missing)
        self.assertEqual(self.reader.call_count, 3)

    def test_sql_observer_uses_only_fixed_read_query_and_private_stdin_credentials(self):
        (self.root / "config").mkdir()
        (self.root / "config/runtime-sql-password").write_bytes(b"synthetic-private-password\n")
        raw = json.dumps(self.observation).encode()
        with patch("live_receipts.subprocess.run", return_value=SimpleNamespace(
            returncode=0, stdout=raw, stderr=b"")) as run:
            self.assertEqual(sql_projection(self.runtime), self.observation)
        self.assertNotIn("synthetic-private-password", str(run.call_args.args))
        self.assertEqual(run.call_args.kwargs["input"], b"synthetic-private-password\n")
        self.assertEqual(run.call_args.args[0][:4], ["docker", "exec", "-i", "a" * 64])
        self.assertNotIn("-h-1", run.call_args.args[0][-1])


if __name__ == "__main__":
    unittest.main()
