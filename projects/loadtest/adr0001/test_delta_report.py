import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import delta_report
from delta_report import calibration_summary, known_sum, observed_control_peaks, observed_resources


class ObservationReportingTests(unittest.TestCase):
    def test_missing_memory_cpu_and_control_do_not_become_zero(self):
        samples = [{"containers": [{"service": "collector1", "memory_usage_bytes": None,
                                    "cpu_interval": None}]}]
        role = observed_resources(samples)["collector1"]
        self.assertIsNone(role["memory_peak_bytes"])
        self.assertIsNone(role["cpu_seconds"])
        self.assertEqual(role["cpu_interval_samples"], 0)
        self.assertEqual(observed_control_peaks(samples), (None, None))
        self.assertIsNone(known_sum([1, None]))
        self.assertIsNone(known_sum([]))

    def test_observed_zero_is_real_and_partial_coverage_is_counted(self):
        samples = [
            {"containers": [{"service": "collector1", "memory_usage_bytes": None, "cpu_interval": None}]},
            {"error": "sampling-error"},
            {"containers": [{"service": "collector1", "memory_usage_bytes": 0,
                             "cpu_interval": {"cpu_usage_ns": 0}}]},
            {"containers": [{"service": "collector1", "memory_usage_bytes": 1024,
                             "cpu_interval": {"cpu_usage_ns": 1_000_000_000}}]},
        ]
        role = observed_resources(samples)["collector1"]
        self.assertEqual((role["cpu_seconds"], role["memory_peak_bytes"]), (1, 1024))
        self.assertEqual((role["samples"], role["cpu_interval_samples"], role["memory_samples"]), (3, 2, 2))
        self.assertEqual(known_sum([0, 1]), 1)

    def test_absent_member_activity_is_not_inferred_idle(self):
        samples = [{"control_observation": {"members": [{"id": "collector1"}]}}]
        self.assertEqual(observed_control_peaks(samples), (None, None))
        samples.append({"control_observation": {"members": [{"active": 0}], "assignments": []}})
        self.assertEqual(observed_control_peaks(samples), (0, 0))

    def test_invalid_counter_is_rejected_not_silently_reported(self):
        with self.assertRaises(AssertionError):
            observed_resources([{"containers": [{"service": "collector1", "cpu_interval": {"cpu_usage_ns": -1}}]}])

    def test_fresh_calibration_requires_matched_binaries_and_exact_sql_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            root = repo / "target/publication-gate/test"
            (root / "observations").mkdir(parents=True)
            binaries = {"test-binary": "a" * 64}
            entry = {"evidence": "target/publication-gate/test", "status": "completed-accounted-phases",
                     "arm": "adr", "tracing": True}
            records, receipts = [], []
            for index in range(6):
                final = {"operation": f"unit-{index}", "version": index, "digest": "b" * 64}
                records.append({"table": "main", "payload_retries": 0, "completion": "stream-final",
                                "terminal": "final", "terminal_ms": index + 1, "receipts": [final],
                                "phase": "cold" if index < 2 else "warm", "rows": 4096})
                receipts.append({"table_id": "main", "operation_id": final["operation"],
                                 "version": final["version"], "digest": final["digest"]})
            receipts.extend([{"table_id": "main", "operation_id": f"unit-{i}-schema"} for i in range(2)])
            for name, value in (
                ("benchmark-inputs.json", records), ("measured-binaries.json", binaries),
                ("phases.json", [{"phase": name, "jobs": jobs, "concurrency": concurrency,
                                  "rows_per_job": rows, "terminal": delta_report.distribution([
                                      record["terminal_ms"] for record in records if record["phase"] == name])}
                                 for name, jobs, concurrency, rows in delta_report.CALIBRATION_PHASES]),
                ("independent-reconciliation.json", {"main": {"missing": 0, "extra_or_duplicate": 0,
                                                            "actual_rows": 24576, "expected_rows": 24576}}),
                ("sql-final-receipts.json", receipts),
            ):
                (root / name).write_text(json.dumps(value))
            (root / "observations/samples.jsonl").write_text('{"error":"synthetic-sample-error"}\n')
            with patch.object(delta_report, "REPO", repo):
                result = calibration_summary(entry, binaries)
                self.assertEqual((result["inputs"], result["completed_inputs"], result["sampling_errors"]), (6, 6, 1))
                self.assertNotIn("synthetic-sample-error", str(result))
                with self.assertRaises(AssertionError):
                    calibration_summary(entry, {"test-binary": "c" * 64})
                receipts[0]["digest"] = "d" * 64
                (root / "sql-final-receipts.json").write_text(json.dumps(receipts))
                with self.assertRaises(AssertionError):
                    calibration_summary(entry, binaries)


if __name__ == "__main__":
    unittest.main()
