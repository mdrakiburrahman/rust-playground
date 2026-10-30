import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from exchange import load
import export_traces


class ExportTests(unittest.TestCase):
    def evidence(self, root):
        cohort = root / "target" / "delta-benchmark" / "test"
        cohort.mkdir(parents=True)
        run = root / "target" / "test-run"
        (run / "logs").mkdir(parents=True)
        (run / "observations").mkdir()
        (cohort / "attempts.json").write_text(json.dumps([
            {"evidence": "target/test-run", "arm": "adr", "repetition": 0}]))
        (cohort / "revision.json").write_text(json.dumps({"commit": "a" * 40}))
        receipt = {"operation": "unit-a", "kind": "DELTA_COMMITTED",
                   "version": 1, "digest": "b" * 64}
        (run / "benchmark-inputs.json").write_text(json.dumps([{
            "trace_id": "trace-a", "phase": "cold", "table": "main", "rows": 1,
            "logical_arrow_bytes": 8, "terminal": "final", "terminal_ms": 2,
            "completion": "stream-final", "receipts": [
                {"operation": "unit-a", "kind": "DELTA_OPERATION", "final": False},
                receipt, {**receipt, "kind": "DELTA_RESOLVED"}],
        }]))
        events = [{
            "event": "service_span", "id": f"span-{i}", "trace_id": "trace-a",
            "role": role, "stage": stage, "category": "lifecycle", "status": "completed",
            "process_id": f"private-{role}", "clock_id": f"private-clock-{role}",
            "offset_ns": "0", "duration_ns": "1",
        } for i, (role, stage) in enumerate(export_traces.REQUIRED_STAGES | {("gateway", "flight-admission")})]
        (run / "logs" / "roles.log").write_text("\n".join(map(json.dumps, events)))
        samples = []
        for at, used in ((100, 10), (200, 30)):
            samples.append({"containers": [{
                "id": "private-container", "engine_read_ns": at, "cpu_total_usage_ns": used,
                "memory_usage_bytes": 4096,
            }]})
        (run / "observations" / "samples.jsonl").write_text("\n".join(map(json.dumps, samples)))
        return run

    def test_roundtrip_resources_and_receipt_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.evidence(root)
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertTrue(result["coverage"]["complete"])
            self.assertEqual(len([r for r in result["records"] if r["kind"] == "unit"]), 1)
            self.assertEqual(len([r for r in result["records"] if r["kind"] == "commit"]), 1)
            cpu = next(r for r in result["records"] if r.get("metric") == "cpu")
            self.assertEqual((cpu["value"], cpu["duration_ns"]), (20, "100"))
            self.assertNotIn("private-container", json.dumps(result))

    def test_missing_stage_is_not_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            (run / "logs" / "roles.log").write_text("")
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertFalse(result["coverage"]["complete"])
            self.assertTrue(result["coverage"]["unobserved_stages"])

    def test_producer_array_bound(self):
        records = export_traces.BoundedList(1)
        records.append({})
        with self.assertRaises(ValueError):
            records.append({})

    def test_timeout_then_resolved_preserves_completion_milestones(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "benchmark-inputs.json"
            inputs = json.loads(path.read_text())
            inputs[0].update(terminal="unknown", terminal_ms=2, completion="identity-resolved-final",
                             admission_identity_observed_ms=1, resolved_receipt_ms=200,
                             reader_visibility_ms=220, reader_scan_ms=10)
            path.write_text(json.dumps(inputs))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            records = result["records"]
            self.assertEqual(next(r for r in records if r["kind"] == "attempt")["status"], "completed")
            terminal = next(r for r in records if r.get("stage") == "rpc-terminal")
            self.assertEqual((terminal["duration_ns"], terminal["status"]), ("2000000", "unknown"))
            resolved = next(r for r in records if r.get("stage") == "resolved-receipt-observed")
            self.assertEqual((resolved["duration_ns"], resolved["status"]), ("200000000", "completed"))
            scan = next(r for r in records if r.get("stage") == "ordinary-reader-scan")
            self.assertEqual((scan["offset_ns"], scan["duration_ns"]), ("210000000", "10000000"))

    def test_distinct_service_clocks_never_create_parent_link(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "logs" / "roles.log"
            spans = [json.loads(line) for line in path.read_text().splitlines()]
            spans[1].update(process_id=spans[0]["process_id"], clock_id="distinct-clock", parent_id=spans[0]["id"])
            path.write_text("\n".join(map(json.dumps, spans)))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertFalse(any(link["relation"] == "parent" for link in result["links"]))
            self.assertTrue(any(link["relation"] == "async" for link in result["links"]))

    def test_mixed_stage_categories_and_outcomes_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "logs" / "roles.log"
            spans = [json.loads(line) for line in path.read_text().splitlines()]
            spans[0].update(category="lifecycle", status="failed")
            spans[1].update(category="downstream_wait", status="cancelled")
            path.write_text("\n".join(map(json.dumps, spans)))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            service = [r for r in result["records"] if r["kind"] == "span" and r["role"] != "client"]
            self.assertEqual((service[0]["category"], service[0]["status"]), ("lifecycle", "failed"))
            self.assertEqual((service[1]["category"], service[1]["status"]), ("downstream_wait", "cancelled"))


if __name__ == "__main__":
    unittest.main()
