import unittest
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace

from table_count_sweep import configure_tables, distribution, summary, table_specs


class TableSweep(unittest.TestCase):
    def test_distinct_immutable_keys_share_only_schema(self):
        tables = table_specs(8, "schema-fingerprint")
        self.assertEqual(len({t["key"] for t in tables}), 8)
        self.assertEqual(len({tuple(t["path"]) for t in tables}), 8)
        self.assertTrue(all(t["fingerprints"] == ["schema-fingerprint"] for t in tables))
        with self.assertRaises(ValueError):
            table_specs(3, "schema-fingerprint")

    def test_rpc_and_eventual_physical_completion_remain_separate(self):
        rows = [
            {"completion": "stream-final", "terminal": "final", "terminal_ms": 5,
             "final_receipt_ms": 4, "reader_visibility_ms": 6, "rows": 4096},
            {"completion": "identity-resolved-final", "terminal": "timeout", "terminal_ms": 7,
             "resolved_receipt_ms": 9, "reader_visibility_ms": 10, "rows": 4096},
            {"completion": "unresolved", "terminal": "failed", "rows": 4096},
        ]
        result = summary(rows, 2)
        self.assertEqual(result["rows_per_second"], 4096)
        self.assertEqual(result["logical_payload_MiB_per_second"], 0.03125)
        self.assertEqual(result["rpc_final_elapsed"]["n"], 1)
        self.assertEqual(result["physical_completion_receipt_observed"]["n"], 2)
        self.assertEqual(result["rpc_outcomes"], {"final": 1, "timeout": 1, "failed": 1})

    def test_small_samples_have_no_supported_tails_or_missing_zeros(self):
        self.assertIsNone(distribution([])["p50_ms"])
        self.assertIsNone(distribution([1, 2])["p95_ms"])

    def test_prestart_allowlist_update_preserves_readonly_collector_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "config").mkdir()
            path = root / "config/collector1.json"
            path.write_text(json.dumps({"tables": [], "operator_token": ""}))
            path.chmod(0o444)
            runtime = SimpleNamespace(root=root, config={}, fingerprints=["schema"])
            try:
                configure_tables(runtime, 8)
                self.assertEqual(len(json.loads(path.read_text())["tables"]), 8)
                self.assertEqual(path.stat().st_mode & 0o777, 0o444)
            finally:
                path.chmod(0o644)


if __name__ == "__main__":
    unittest.main()
