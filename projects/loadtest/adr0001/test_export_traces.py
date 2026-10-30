import copy
import hashlib
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
                "observer_clock_id": "test-observer", "observer_origin_ns": 100,
                "observed_monotonic_ns": at,
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
            self.assertTrue(result["provenance"]["synthetic"])
            spans = [record for record in result["records"] if record["kind"] == "span" and record["role"] != "client"]
            self.assertTrue(spans)
            self.assertTrue(all(record["trace_id"] == "attempt-0-0" for record in spans))
            span_ids = {record["id"] for record in spans}
            self.assertFalse(any(link["relation"] == "contains" and link["to"] in span_ids for link in result["links"]))

    def test_measured_claim_requires_frozen_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.evidence(root)
            revision = root / "target/delta-benchmark/test/revision.json"
            revision.write_text(json.dumps({"commit": "a" * 40, "measurement": "real-sql-delta"}))
            with patch.object(export_traces, "REPO", root), self.assertRaisesRegex(ValueError, "frozen"):
                export_traces.export("test")

    def test_measured_export_keeps_canonical_vocabulary_and_binds_postprocessor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            cohort = root / "target/delta-benchmark/test"
            manifest = cohort / "source-binary-sha256.json"
            manifest.write_text(json.dumps({"test-source": "c" * 64}))
            (cohort / "revision.json").write_text(json.dumps({
                "commit": "a" * 40, "measurement": "real-sql-delta",
                "source_binary_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            }))
            attempts = json.loads((cohort / "attempts.json").read_text())
            attempts[0]["status"] = "completed-accounted-phases"
            (cohort / "attempts.json").write_text(json.dumps(attempts))
            (run / "independent-reconciliation.json").write_text(json.dumps({
                "main": {"missing": 0, "extra_or_duplicate": 0},
            }))
            (run / "sql-final-receipts.json").write_text(json.dumps([{
                "table_id": "main", "operation_id": "unit-a", "version": 1, "digest": "b" * 64,
            }]))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertFalse(result["provenance"]["synthetic"])
            self.assertEqual(result["provenance"]["description"], "finite-local-comparison")
            self.assertIn(hashlib.sha256(Path(export_traces.__file__).read_bytes()).hexdigest(),
                          result["provenance"]["artifact_sha256"])
            (run / "sql-final-receipts.json").write_text("[]")
            with patch.object(export_traces, "REPO", root), self.assertRaisesRegex(ValueError, "SQL receipt"):
                export_traces.export("test")

    def test_missing_stage_is_not_complete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            (run / "logs" / "roles.log").write_text("")
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertFalse(result["coverage"]["complete"])
            self.assertTrue(result["coverage"]["unobserved_stages"])

    def test_incomplete_attempt_is_counted_once_despite_multiple_missing_surfaces(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "benchmark-inputs.json"
            records = json.loads(path.read_text())
            records[0].update(completion="unresolved", terminal="timeout",
                              receipts=[records[0]["receipts"][0]])
            path.write_text(json.dumps(records))
            (run / "logs/roles.log").write_text("")
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertEqual(result["coverage"]["incomplete_attempts"], 1)
            self.assertFalse(result["coverage"]["complete"])

    def test_deadline_cleanup_exception_requires_complete_hash_bound_independent_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            cohort = repo / "target/delta-benchmark/test"
            cohort.mkdir(parents=True)
            attempts, audited = [], []
            for index in range(6):
                root = repo / f"target/publication-gate/run-{index}"
                (root / "observations").mkdir(parents=True)
                reconciliation = {"main": {"missing": 0, "extra_or_duplicate": 0}}
                data = {
                    "benchmark-inputs.json": [{}] * 56, "phases.json": [{}] * 9,
                    "independent-reconciliation.json": reconciliation, "sql-final-receipts.json": [],
                    "substrate-costs.json": {}, "measured-binaries.json": {},
                    "stopped-containers.json": [{"State": {"Running": False}}],
                    "observations/intervals.json": {}, "observations/samples.jsonl": [], "sql-cgroup.json": [],
                }
                for name, value in data.items():
                    (root / name).write_text(json.dumps(value))
                entry = {"evidence": str(root.relative_to(repo)),
                         "status": "blocked-cleanup" if index == 5 else "completed-accounted-phases"}
                attempts.append(entry)
                scan = cohort / f"post-stop-rescan-r{index}.json"
                scan.write_text(json.dumps(reconciliation))
                audited.append({
                    "evidence": entry["evidence"], "raw_status": entry["status"],
                    "files": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in data},
                    "independent_rescan_sha256": hashlib.sha256(scan.read_bytes()).hexdigest(),
                })
            (cohort / "attempts.json").write_text(json.dumps(attempts))
            for name in ("calibration.json", "source-binary-sha256.json", "deadline-cleanup-recovery.json"):
                (cohort / name).write_text("{}")
            with patch.object(export_traces, "REPO", repo), self.assertRaisesRegex(ValueError, "completed"):
                export_traces.require_completed_measurement(cohort, attempts[-1])
            audit = {
                "schema_version": 1, "measurement_status": "all-six-nine-phase-measurements-verified",
                "driver_exit_code": 124, "driver_outcome": "deadline-during-final-container-cleanup",
                "raw_metadata_unchanged": True, "runs": audited,
            }
            for key, name in (
                ("attempts_sha256", "attempts.json"), ("calibration_sha256", "calibration.json"),
                ("frozen_manifest_sha256", "source-binary-sha256.json"),
                ("cleanup_recovery_sha256", "deadline-cleanup-recovery.json"),
            ):
                audit[key] = hashlib.sha256((cohort / name).read_bytes()).hexdigest()
            (cohort / "measurement-audit.json").write_text(json.dumps(audit))
            with patch.object(export_traces, "REPO", repo):
                export_traces.require_completed_measurement(cohort, attempts[-1])
                self.assertEqual(attempts[-1]["status"], "blocked-cleanup")
                (repo / attempts[0]["evidence"] / "phases.json").write_text("[]")
                with self.assertRaisesRegex(ValueError, "evidence changed"):
                    export_traces.require_completed_measurement(cohort, attempts[-1])

    def test_resource_intervals_ignore_engine_wall_clock_rollback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "observations" / "samples.jsonl"
            samples = [json.loads(line) for line in path.read_text().splitlines()]
            samples[1]["containers"][0]["engine_read_ns"] = 50
            path.write_text("\n".join(map(json.dumps, samples)))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            cpu = next(record for record in result["records"] if record.get("metric") == "cpu")
            self.assertEqual((cpu["value"], cpu["duration_ns"]), (20, "100"))

    def test_historical_resource_wall_clock_is_not_a_monotonic_interval(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "observations" / "samples.jsonl"
            samples = [json.loads(line) for line in path.read_text().splitlines()]
            for sample in samples:
                for field in ("observer_clock_id", "observer_origin_ns", "observed_monotonic_ns"):
                    sample["containers"][0].pop(field)
            path.write_text("\n".join(map(json.dumps, samples)))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertFalse(any(record.get("metric") == "cpu" for record in result["records"]))
            self.assertTrue(any("process-clock" in stage for stage in result["coverage"]["unobserved_stages"]))

    def test_explicit_counter_gap_preserves_memory_but_never_fabricates_cpu(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "observations/samples.jsonl"
            samples = [json.loads(line) for line in path.read_text().splitlines()]
            samples[1].update(error="counter gap", sampling_errors=1)
            samples[1]["containers"][0].update(cpu_total_usage_ns=0, cpu_interval_error="invalid-counter-interval")
            after = copy.deepcopy(samples[0])
            after["containers"][0].update(observed_monotonic_ns=300, cpu_total_usage_ns=50)
            samples.append(after)
            path.write_text("\n".join(map(json.dumps, samples)))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertFalse(any(record.get("metric") == "cpu" for record in result["records"]))
            self.assertEqual(len([record for record in result["records"] if record.get("metric") == "memory"]), 3)
            self.assertEqual(result["coverage"]["dropped_events"], 1)
            self.assertFalse(result["coverage"]["complete"])

    def test_unmatched_background_span_is_counted_not_silently_discarded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "logs" / "roles.log"
            span = json.loads(path.read_text().splitlines()[0])
            span.update(id="background-span", trace_id="not-an-input", stage="background-recovery")
            with path.open("a") as stream:
                stream.write("\n" + json.dumps(span) + "\n" + json.dumps(span))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertFalse(result["coverage"]["complete"])
            self.assertEqual(result["coverage"]["dropped_events"], 1)
            self.assertTrue(any("unmatched-service-span" in stage
                                for stage in result["coverage"]["unobserved_stages"]))

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

    def test_multiple_attempts_share_the_same_logical_unit_and_physical_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "benchmark-inputs.json"
            inputs = json.loads(path.read_text())
            repeated = copy.deepcopy(inputs[0])
            repeated["trace_id"] = "trace-b"
            inputs.append(repeated)
            path.write_text(json.dumps(inputs))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertEqual(len([r for r in result["records"] if r["kind"] == "attempt"]), 2)
            self.assertEqual(len([r for r in result["records"] if r["kind"] == "unit"]), 1)
            self.assertEqual(len([r for r in result["records"] if r["kind"] == "commit"]), 1)
            unit = next(r["id"] for r in result["records"] if r["kind"] == "unit")
            batches = [link for link in result["links"] if link["to"] == unit and link["relation"] == "contains"]
            self.assertEqual(len(batches), 2)
            self.assertEqual(len([link for link in result["links"] if link["relation"] == "publishes"]), 1)

    def test_multiple_units_can_share_one_physical_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "benchmark-inputs.json"
            inputs = json.loads(path.read_text())
            shared = copy.deepcopy(inputs[0])
            shared["trace_id"] = "trace-b"
            for receipt in shared["receipts"]:
                receipt["operation"] = "unit-b"
            inputs.append(shared)
            path.write_text(json.dumps(inputs))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            self.assertEqual(len([r for r in result["records"] if r["kind"] == "unit"]), 2)
            self.assertEqual(len([r for r in result["records"] if r["kind"] == "commit"]), 1)
            self.assertEqual(len([link for link in result["links"] if link["relation"] == "publishes"]), 2)

    def test_conflicting_physical_commit_digests_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "benchmark-inputs.json"
            inputs = json.loads(path.read_text())
            inputs[0]["receipts"][-1]["digest"] = "c" * 64
            path.write_text(json.dumps(inputs))
            with patch.object(export_traces, "REPO", root), self.assertRaisesRegex(ValueError, "commit digest"):
                export_traces.export("test")

    def test_transport_failure_status_is_separate_from_eventual_completion(self):
        for status in ("failed", "cancelled", "timeout", "rejected"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                run = self.evidence(root)
                path = run / "benchmark-inputs.json"
                inputs = json.loads(path.read_text())
                inputs[0].update(terminal=status, completion="identity-resolved-final", resolved_receipt_ms=200)
                path.write_text(json.dumps(inputs))
                with patch.object(export_traces, "REPO", root):
                    result = load(export_traces.export("test"))
                terminal = next(r for r in result["records"] if r.get("stage") == "rpc-terminal")
                self.assertEqual(terminal["status"], status)
                self.assertEqual(next(r for r in result["records"] if r["kind"] == "attempt")["status"], "completed")

    def test_client_clock_identity_and_offsets_survive_export(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = self.evidence(root)
            path = run / "benchmark-inputs.json"
            inputs = json.loads(path.read_text())
            inputs[0].update(client_clock_id="private-clock", client_process_id="private-process",
                             client_offset_ns="100", reader_visibility_ms=220, reader_scan_ms=10)
            repeated = copy.deepcopy(inputs[0])
            repeated.update(trace_id="trace-b", client_offset_ns="200")
            inputs.append(repeated)
            path.write_text(json.dumps(inputs))
            with patch.object(export_traces, "REPO", root):
                result = load(export_traces.export("test"))
            terminals = [r for r in result["records"] if r.get("stage") == "rpc-terminal"]
            self.assertEqual(terminals[0]["clock_id"], terminals[1]["clock_id"])
            self.assertEqual([r["offset_ns"] for r in terminals], ["100", "200"])
            scans = [r for r in result["records"] if r.get("stage") == "ordinary-reader-scan"]
            self.assertEqual([r["offset_ns"] for r in scans], ["210000100", "210000200"])
            self.assertNotIn("private-clock", json.dumps(result))
            self.assertNotIn("private-process", json.dumps(result))

    def test_input_span_never_subtracts_wall_time_or_different_clocks(self):
        records = [{"client_clock_id": "clock", "started_monotonic_ns": 100, "ended_monotonic_ns": 200,
                    "started_ns": 10000, "ended_ns": 1},
                   {"client_clock_id": "clock", "started_monotonic_ns": 150, "ended_monotonic_ns": 400}]
        self.assertEqual(export_traces.client_input_span_seconds(records), 300 / 1_000_000_000)
        records[1]["client_clock_id"] = "other-clock"
        with self.assertRaisesRegex(ValueError, "clock domains"):
            export_traces.client_input_span_seconds(records)
        self.assertIsNone(export_traces.client_input_span_seconds([{"started_ns": 1, "ended_ns": 2}]))
        with self.assertRaisesRegex(ValueError, "monotonic interval"):
            export_traces.client_input_span_seconds(
                [{"client_clock_id": "clock", "started_monotonic_ns": 200, "ended_monotonic_ns": 100}])


if __name__ == "__main__":
    unittest.main()
