"""Allowlisted projection of finite local traces; never copies runtime configuration."""
import argparse
from collections import defaultdict
import hashlib
import html
from itertools import chain
import json
from pathlib import Path
import re
import statistics

from exchange import validate, MAX_BYTES

REPO = Path(__file__).resolve().parents[3]

REQUIRED_STAGES = {
    ("collector", "collector-stream"), ("collector", "receive-decode"),
    ("collector", "collector-mailbox"), ("collector", "encode-stage"),
    ("publisher", "publisher-request"), ("publisher", "data-materialize"),
    ("authority", "sql-transaction"),
}

class BoundedList(list):
    def __init__(self, limit):
        super().__init__()
        self.limit = limit

    def append(self, item):
        if len(self) >= self.limit:
            raise ValueError("export array limit")
        super().append(item)

    def extend(self, items):
        for item in items:
            self.append(item)


def bounded_bytes(path):
    with path.open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError(f"oversized evidence file: {path.name}")
    return raw


def bounded_json(path):
    return json.loads(bounded_bytes(path))


def require_completed_measurement(directory, attempt):
    if attempt.get("status") == "completed-accounted-phases":
        return
    audit_path = directory / "measurement-audit.json"
    if attempt.get("status") != "blocked-cleanup" or not audit_path.is_file():
        raise ValueError("measured export requires a completed, reconciled cohort")
    audit = bounded_json(audit_path)
    attempts = bounded_json(directory / "attempts.json")
    if (len(attempts) != 6 or attempts[-1] != attempt
            or [a["status"] for a in attempts] != ["completed-accounted-phases"] * 5 + ["blocked-cleanup"]
            or audit.get("schema_version") != 1
            or audit.get("measurement_status") != "all-six-nine-phase-measurements-verified"
            or audit.get("driver_exit_code") != 124
            or audit.get("driver_outcome") != "deadline-during-final-container-cleanup"
            or audit.get("raw_metadata_unchanged") is not True):
        raise ValueError("unqualified measurement cleanup audit")
    bindings = {
        "attempts_sha256": "attempts.json", "calibration_sha256": "calibration.json",
        "frozen_manifest_sha256": "source-binary-sha256.json",
        "cleanup_recovery_sha256": "deadline-cleanup-recovery.json",
    }
    if any(audit.get(key) != hashlib.sha256(bounded_bytes(directory / name)).hexdigest()
           for key, name in bindings.items()):
        raise ValueError("measurement audit metadata changed")
    rows = audit.get("runs", [])
    if len(rows) != 6 or len({row["evidence"] for row in rows}) != 6:
        raise ValueError("measurement audit lacks six distinct runs")
    required = {
        "benchmark-inputs.json", "phases.json", "independent-reconciliation.json",
        "sql-final-receipts.json", "substrate-costs.json", "measured-binaries.json",
        "stopped-containers.json", "observations/intervals.json",
        "observations/samples.jsonl", "sql-cgroup.json",
    }
    for index, (entry, row) in enumerate(zip(attempts, rows)):
        if row["evidence"] != entry["evidence"] or row["raw_status"] != entry["status"]:
            raise ValueError("measurement audit run changed")
        root = (REPO / entry["evidence"]).resolve()
        if not root.is_relative_to((REPO / "target/publication-gate").resolve()):
            raise ValueError("measurement audit outside dedicated evidence root")
        files = row["files"]
        if not required <= files.keys() or any(
            name not in required and not re.fullmatch(r"logs/[a-z0-9-]+\.log", name) for name in files
        ):
            raise ValueError("measurement audit file scope")
        if any(hashlib.sha256(bounded_bytes(root / name)).hexdigest() != expected for name, expected in files.items()):
            raise ValueError("audited measurement evidence changed")
        if len(bounded_json(root / "benchmark-inputs.json")) != 56 or len(bounded_json(root / "phases.json")) != 9:
            raise ValueError("audited measurement is incomplete")
        inventory = bounded_json(root / "stopped-containers.json")
        if not inventory or any(component["State"]["Running"] for component in inventory):
            raise ValueError("audited cleanup not complete")
        scan_path = directory / f"post-stop-rescan-r{index}.json"
        if hashlib.sha256(bounded_bytes(scan_path)).hexdigest() != row["independent_rescan_sha256"]:
            raise ValueError("audited independent rescan changed")
        scan, original = bounded_json(scan_path), bounded_json(root / "independent-reconciliation.json")
        if not scan or any(value != original.get(key) or value["missing"] or value["extra_or_duplicate"]
                           for key, value in scan.items()):
            raise ValueError("audited independent Delta reconciliation failed")


def client_input_span_seconds(records):
    fields = ("client_clock_id", "started_monotonic_ns", "ended_monotonic_ns")
    if not records or any(any(field not in record for field in fields) for record in records):
        return None
    if len({record["client_clock_id"] for record in records}) != 1:
        raise ValueError("client input span crosses clock domains")
    for record in records:
        start, end = record["started_monotonic_ns"], record["ended_monotonic_ns"]
        if type(start) is not int or type(end) is not int or not 0 <= start <= end:
            raise ValueError("invalid client monotonic interval")
    return (max(r["ended_monotonic_ns"] for r in records)
            - min(r["started_monotonic_ns"] for r in records)) / 1_000_000_000


def resource_records(bundle, root, run_id):
    """Container-scoped counters; Engine timestamps are not service clocks."""
    previous, origins = {}, {}
    samples = root / "observations" / "samples.jsonl"
    if not samples.exists():
        bundle["coverage"]["unobserved_stages"].append(f"{run_id}-process-resources")
        return
    def read_samples():
        with samples.open("rb") as stream:
            while line := stream.readline(MAX_BYTES + 1):
                if len(line) > MAX_BYTES:
                    raise ValueError("oversized resource sample")
                yield json.loads(line)

    sql = root / "sql-cgroup.json"
    sql_samples = ({"containers": [{**sample, "id": "sql", "service": "sql"}]}
                   for sample in bounded_json(sql)) if sql.exists() else ()
    for sample in chain(read_samples(), sql_samples):
        if "error" in sample:
            bundle["coverage"]["dropped_events"] += sample.get("sampling_errors", 1)
        for component in sample.get("containers", []):
            source = component["id"]
            process = f"{run_id}-cgroup-{hashlib.sha256(source.encode()).hexdigest()[:16]}"
            if any(field not in component for field in ("observer_clock_id", "observer_origin_ns", "observed_monotonic_ns")):
                bundle["coverage"]["unobserved_stages"].append(f"{run_id}-process-clock")
                continue
            clock_source = component["observer_clock_id"]
            at = component["observed_monotonic_ns"]
            if type(at) is not int or at < 0:
                raise ValueError("invalid observer monotonic time")
            clock = f"{run_id}-observer-{hashlib.sha256(clock_source.encode()).hexdigest()[:16]}"
            origin = origins.setdefault(clock_source, component["observer_origin_ns"])
            if type(origin) is not int or origin < 0 or origin != component["observer_origin_ns"]:
                raise ValueError("invalid observer clock origin")
            if at < origin:
                raise ValueError("resource clock moved backwards")
            def add(metric, unit, value, offset, duration):
                if value is None:
                    bundle["coverage"]["unobserved_stages"].append(f"{run_id}-process-{metric}")
                    return
                bundle["records"].append({
                    "id": f"resource-{len(bundle['records'])}", "run_id": run_id,
                    "kind": "resource", "evidence": "measured", "process_id": process,
                    "clock_id": clock, "metric": metric, "unit": unit,
                    "value": value, "offset_ns": str(offset), "duration_ns": str(duration),
                    "method": "process-counter", "assumption": "process-not-request",
                })
            add("memory", "bytes", component.get("memory_usage_bytes"), at - origin, 0)
            if component.get("cpu_interval_error"):
                bundle["coverage"]["unobserved_stages"].append(f"{run_id}-process-cpu-gap")
                previous.pop(source, None)
                continue
            old = previous.get(source)
            if old is not None:
                if old["observer_clock_id"] != clock_source:
                    raise ValueError("resource interval crosses observer clocks")
                duration = at - old["observed_monotonic_ns"]
                used = component["cpu_total_usage_ns"] - old["cpu_total_usage_ns"]
                if duration <= 0 or used < 0:
                    raise ValueError("invalid resource counter interval")
                add("cpu", "ns", used, old["observed_monotonic_ns"] - origin, duration)
            previous[source] = component


def export(cohort):
    directory = REPO / "target/delta-benchmark" / cohort
    attempts = bounded_json(directory / "attempts.json")
    revision = bounded_json(directory / "revision.json")
    measured = revision.get("measurement") == "real-sql-delta"
    if measured and "source_binary_manifest_sha256" not in revision:
        raise ValueError("measured cohort lacks frozen source/binary manifest")
    bundle = {
        "format": "arrowcol-benchmark", "schema_version": 1, "dataset_id": cohort,
        "provenance": {"source_revision": revision["commit"],
                       "synthetic": not measured,
                       "description": "finite-local-comparison"},
        "runs": [], "records": BoundedList(100000), "links": BoundedList(200000),
        "coverage": {"dropped_events": 0, "incomplete_attempts": 0, "complete": True,
                     "unobserved_stages": ["one-way-network", "sql-engine-cpu", "per-request-cpu"]},
    }
    if "source_binary_manifest_sha256" in revision:
        manifest_path = directory / "source-binary-sha256.json"
        manifest_hash = hashlib.sha256(bounded_bytes(manifest_path)).hexdigest()
        if manifest_hash != revision["source_binary_manifest_sha256"]:
            raise ValueError("source/binary provenance manifest changed")
        bundle["provenance"]["artifact_sha256"] = [
            manifest_hash, hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        ]
        if any(attempt.get("status") == "blocked-cleanup" for attempt in attempts):
            require_completed_measurement(directory, attempts[-1])
            bundle["provenance"]["artifact_sha256"].append(
                hashlib.sha256(bounded_bytes(directory / "measurement-audit.json")).hexdigest())
            bundle["coverage"]["unobserved_stages"].append("driver-final-cleanup-interrupted")
    elif revision.get("dirty"):
        raise ValueError("dirty measurement lacks source/binary manifest")
    stages = defaultdict(list)
    incomplete_attempts = set()
    for index, attempt in enumerate(attempts):
        root = REPO / attempt["evidence"]
        inputs = bounded_json(root / "benchmark-inputs.json")
        if measured:
            require_completed_measurement(directory, attempt)
            reconciliation = bounded_json(root / "independent-reconciliation.json")
            sql = bounded_json(root / "sql-final-receipts.json")
            completions = {(receipt["table_id"], receipt["operation_id"]): receipt for receipt in sql}
            for table in {record["table"] for record in inputs}:
                value = reconciliation.get(table)
                if not isinstance(value, dict) or value.get("missing") != 0 or value.get("extra_or_duplicate") != 0:
                    raise ValueError("ordinary Delta multiset reconciliation failed")
            for record in inputs:
                for receipt in record["receipts"]:
                    if receipt["kind"] not in ("DELTA_COMMITTED", "DELTA_RESOLVED"):
                        continue
                    actual = completions.get((record["table"], receipt["operation"]))
                    if (actual is None or actual["version"] != receipt["version"]
                            or actual["digest"] != receipt["digest"]):
                        raise ValueError("SQL receipt reconciliation failed")
        traces = {}
        observed = defaultdict(set)
        operation_units = {}
        commits = {}
        commit_digests = {}
        published_links = set()
        run_id = f"r{index}"
        def commit_for(table, version, digest, stage=None):
            identity = (table, version)
            if identity in commit_digests and commit_digests[identity] != digest:
                raise ValueError("conflicting physical commit digest")
            commit_digests[identity] = digest
            if identity not in commits:
                commit_id = f"commit-{index}-{len(commits)}"
                commits[identity] = commit_id
                item = {"id": commit_id, "run_id": run_id, "kind": "commit", "evidence": "measured",
                        "version": version, "digest": digest, "table_id": f"{run_id}-{table}"}
                if stage is not None:
                    item["stage"] = stage
                bundle["records"].append(item)
            return commits[identity]

        def publishes(unit_id, commit_id):
            if (unit_id, commit_id) not in published_links:
                bundle["links"].append({"from": unit_id, "to": commit_id, "relation": "publishes"})
                published_links.add((unit_id, commit_id))

        bundle["runs"].append({"id": run_id, "arm": attempt["arm"], "scenario": "matched-nine-phases",
                               "repetition": attempt["repetition"], "seed": 0})
        for table in ("main", "cas"):
            bundle["records"].append({"id": f"{run_id}-{table}", "run_id": run_id,
                                      "kind": "table", "evidence": "measured"})
        for record_index, record in enumerate(inputs):
            phase = record["phase"]
            request_id, attempt_id = f"request-{index}-{record_index}", f"attempt-{index}-{record_index}"
            traces[record["trace_id"]] = (run_id, attempt_id)
            bundle["records"].extend([
                {"id": request_id, "run_id": run_id, "kind": "request", "evidence": "measured",
                 "table_id": f"{run_id}-{record['table']}", "rows": record["rows"], "bytes": record["logical_arrow_bytes"]},
                {"id": attempt_id, "run_id": run_id, "kind": "attempt", "evidence": "measured",
                 "status": "completed" if record["completion"] != "unresolved" else "unknown"},
            ])
            bundle["links"].append({"from": request_id, "to": attempt_id, "relation": "attempt"})
            batch_id = f"batch-{index}-{record_index}"
            bundle["records"].append({"id": batch_id, "run_id": run_id, "kind": "batch", "evidence": "measured",
                                      "rows": record["rows"], "stage": phase})
            bundle["links"].append({"from": attempt_id, "to": batch_id, "relation": "contains", "rows": record["rows"]})
            units = set()
            for receipt_index, receipt in enumerate(record["receipts"]):
                receipt_id = f"receipt-{index}-{record_index}-{receipt_index}"
                operation = receipt["operation"]
                identity = (record["table"], operation)
                if identity not in operation_units:
                    unit_id = f"unit-{index}-{len(operation_units)}"
                    operation_units[identity] = unit_id
                    bundle["records"].append({"id": unit_id, "run_id": run_id, "kind": "unit", "evidence": "measured"})
                unit_id = operation_units[identity]
                if unit_id not in units:
                    bundle["links"].append({"from": batch_id, "to": unit_id, "relation": "contains"})
                    units.add(unit_id)
                final = receipt["kind"] in ("DELTA_COMMITTED", "DELTA_RESOLVED") and receipt.get("final", True)
                bundle["records"].append({"id": receipt_id, "run_id": run_id, "kind": "receipt", "evidence": "measured",
                                          "status": "completed" if final else "pending"})
                bundle["links"].append({"from": unit_id, "to": receipt_id, "relation": "receipt"})
                if final:
                    commit_id = commit_for(record["table"], receipt["version"], receipt["digest"])
                    publishes(unit_id, commit_id)
                    bundle["links"].append({"from": commit_id, "to": receipt_id, "relation": "receipt"})
            terminal_status = {
                "final": "completed", "unknown": "unknown", "failed": "failed",
                "cancelled": "cancelled", "timeout": "timeout", "rejected": "rejected",
            }
            if record["terminal"] not in terminal_status:
                raise ValueError("unsupported transport terminal status")
            if "client_clock_id" in record:
                clock_alias = hashlib.sha256(record["client_clock_id"].encode()).hexdigest()[:16]
                process_alias = hashlib.sha256(record["client_process_id"].encode()).hexdigest()[:16]
                client_clock = f"client-{index}-{clock_alias}"
                client_process = f"client-{index}-{process_alias}"
                client_offset = int(record["client_offset_ns"])
                if client_offset < 0:
                    raise ValueError("negative client clock offset")
            else:
                client_clock, client_process = f"client-{index}-{record_index}", f"client-{index}"
                client_offset = 0
            duration = round(record["terminal_ms"] * 1_000_000)
            bundle["records"].append({
                "id": f"client-{index}-{record_index}", "run_id": run_id, "kind": "span", "evidence": "measured",
                "trace_id": attempt_id, "role": "client", "process_id": client_process, "clock_id": client_clock,
                "stage": "rpc-terminal", "category": "transport_observation", "offset_ns": str(client_offset),
                "duration_ns": str(duration),
                "status": terminal_status[record["terminal"]], "method": "local-monotonic",
            })
            milestones = (
                ("admission_identity_observed_ms", "admission-identity-observed"),
                ("final_receipt_ms", "final-receipt-observed"),
                ("resolved_receipt_ms", "resolved-receipt-observed"),
                ("reader_visibility_ms", "reader-visibility-observed"),
            )
            for field, stage in milestones:
                if record.get(field) is None:
                    continue
                elapsed = round(record[field] * 1_000_000)
                bundle["records"].append({
                    "id": f"{stage}-{index}-{record_index}", "run_id": run_id,
                    "kind": "span", "evidence": "measured", "trace_id": attempt_id,
                    "role": "client", "process_id": client_process, "clock_id": client_clock,
                    "stage": stage, "category": "transport_observation", "offset_ns": str(client_offset),
                    "duration_ns": str(elapsed), "status": "completed", "method": "client-milestones",
                })
            if record.get("reader_scan_ms") is not None:
                scan = round(record["reader_scan_ms"] * 1_000_000)
                end = round(record["reader_visibility_ms"] * 1_000_000)
                if end < scan:
                    raise ValueError("invalid reader scan interval")
                bundle["records"].append({
                    "id": f"reader-scan-{index}-{record_index}", "run_id": run_id,
                    "kind": "span", "evidence": "measured", "trace_id": attempt_id,
                    "role": "client", "process_id": client_process, "clock_id": client_clock,
                    "stage": "ordinary-reader-scan", "category": "downstream_wait",
                    "offset_ns": str(client_offset + end - scan), "duration_ns": str(scan), "status": "completed",
                    "method": "local-monotonic",
                })
            if record["completion"] == "unresolved":
                incomplete_attempts.add(attempt_id)
        receipts_path = root / "sql-final-receipts.json"
        if receipts_path.exists():
            for receipt in bounded_json(receipts_path):
                operation = receipt["operation_id"]
                if not operation.endswith("-schema"):
                    continue
                unit = operation_units.get((receipt["table_id"], operation.removesuffix("-schema")))
                if unit is None:
                    raise ValueError("unattributed schema commit")
                commit_id = commit_for(receipt["table_id"], receipt["version"], receipt["digest"], "schema-evolution")
                receipt_id = f"sql-receipt-{len(bundle['records'])}"
                bundle["records"].append(
                    {"id": receipt_id, "run_id": run_id, "kind": "receipt", "evidence": "measured", "status": "completed"})
                publishes(unit, commit_id)
                bundle["links"].append({"from": commit_id, "to": receipt_id, "relation": "receipt"})
        seen = set()
        raw_spans = {}
        parent_refs = []
        for path in sorted((root / "logs").glob("*")):
            if not path.is_file():
                continue
            if path.stat().st_size > MAX_BYTES:
                raise ValueError(f"oversized service log: {path.name}")
            with path.open("rb") as stream:
                raw_log = stream.read(MAX_BYTES + 1)
            if len(raw_log) > MAX_BYTES:
                raise ValueError("oversized service log")
            for line in raw_log.decode(errors="replace").splitlines():
                if not line.startswith("{"):
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError:
                    bundle["coverage"]["dropped_events"] += 1
                    continue
                if raw.get("event") == "trace_overflow":
                    bundle["coverage"]["dropped_events"] += 1
                if raw.get("event") != "service_span" or raw["id"] in seen:
                    continue
                seen.add(raw["id"])
                trace = traces.get(raw.get("trace_id"))
                if not trace:
                    bundle["coverage"]["dropped_events"] += 1
                    bundle["coverage"]["unobserved_stages"].append(f"{run_id}-unmatched-service-span")
                    continue
                run_id, attempt_id = trace
                item = {key: raw[key] for key in ("role", "stage", "category", "status", "offset_ns", "duration_ns")}
                process = f"p{index}-{hashlib.sha256(raw['process_id'].encode()).hexdigest()[:16]}"
                if not raw.get("clock_id"):
                    raise ValueError("service span lacks clock identity")
                clock = f"c{index}-{hashlib.sha256(raw['clock_id'].encode()).hexdigest()[:16]}"
                item.update(id=f"s{index}-{len(seen)}", run_id=run_id, kind="span", evidence="measured",
                            trace_id=attempt_id, process_id=process, clock_id=clock, method="local-monotonic")
                bundle["records"].append(item)
                raw_spans[raw["id"]] = item
                if raw.get("parent_id"):
                    parent_refs.append((raw["parent_id"], item))
                stages[(attempt["arm"], item["role"], item["stage"])].append(int(item["duration_ns"]) / 1_000_000)
                observed[attempt_id].add((item["role"], item["stage"]))
        for parent_id, child in parent_refs:
            parent = raw_spans.get(parent_id)
            if parent is None:
                bundle["coverage"]["unobserved_stages"].append(f"{run_id}-causal-parent")
                bundle["coverage"]["dropped_events"] += 1
                continue
            same_clock = parent["process_id"] == child["process_id"] and parent["clock_id"] == child["clock_id"]
            enclosed = (int(parent["offset_ns"]) <= int(child["offset_ns"])
                        and int(child["offset_ns"]) + int(child["duration_ns"])
                        <= int(parent["offset_ns"]) + int(parent["duration_ns"]))
            bundle["links"].append({"from": parent["id"], "to": child["id"],
                                    "relation": "parent" if same_clock and enclosed else "async"})
        for _, attempt_id in traces.values():
            gateway_stage = "flight-admission" if attempt["arm"] == "adr" else "gateway-stream"
            missing = (REQUIRED_STAGES | {("gateway", gateway_stage)}) - observed[attempt_id]
            if missing:
                incomplete_attempts.add(attempt_id)
                bundle["coverage"]["unobserved_stages"].extend(
                    f"{run_id}-{role}-{stage}" for role, stage in missing)
        resource_records(bundle, root, run_id)
    bundle["coverage"]["unobserved_stages"] = sorted(set(bundle["coverage"]["unobserved_stages"]))
    bundle["coverage"]["incomplete_attempts"] = len(incomplete_attempts)
    bundle["coverage"]["complete"] = not (
        bundle["coverage"]["dropped_events"] or bundle["coverage"]["incomplete_attempts"]
        or "driver-final-cleanup-interrupted" in bundle["coverage"]["unobserved_stages"])
    validate(bundle)
    output = directory / "benchmark-v1.json"
    encoded = json.dumps(bundle, allow_nan=False).encode()
    if len(encoded) > MAX_BYTES:
        raise ValueError("export byte limit")
    output.write_bytes(encoded)
    rows = "".join(f"<tr><td>{html.escape(arm)}</td><td>{html.escape(role)}</td><td>{html.escape(stage)}</td>"
                   f"<td>{len(values)}</td><td>{statistics.mean(values):.3f}</td></tr>"
                   for (arm, role, stage), values in sorted(stages.items()))
    report = (
        "<!doctype html><meta charset=utf-8><title>Measured service stages</title>"
        "<h1>Finite local service-stage measurements</h1>"
        "<p>Stage elapsed time is measured with local monotonic clocks. Nested/concurrent stages overlap: "
        "do not add these rows. Gateway lifetime includes writer wait. SQL round trips are not engine CPU. "
        "No one-way network latency or per-request CPU allocation is claimed.</p>"
        "<p>All stage rows are inclusive elapsed observations, not exclusive local work, even if categorized "
        "processing. Stage means include all retained outcome statuses; inspect the JSON to separate them. "
        "Resource CPU records are cgroup counter deltas over observer-local monotonic receive intervals; "
        "zero-duration memory records are point samples. Their clocks cannot be subtracted from service clocks. "
        "Dropped-event counts include observed overflow markers (a lower bound on suppressed events).</p>"
        "<p>Client milestone spans overlap: each runs from request start to its named observed event. "
        "RPC-terminal status is separate from the attempt's reconciled completion status. Ordinary-reader "
        "scans are separate post-completion intervals, not exact first visibility.</p>"
        "<table><tr><th>Arm</th><th>Role</th><th>Stage</th><th>Samples</th><th>Mean ms</th></tr>" + rows + "</table>"
        "<p>Finite marker workloads are synthetic; new provenance-qualified events are real measurements, "
        "not simulated fixture events. Failures and unknown outcomes remain in the JSON. "
        "Small cohorts do not establish production capacity.</p>"
    )
    (directory / "stage-report.html").write_text(report)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cohort", required=True)
    args = parser.parse_args()
    if not args.cohort.replace("-", "").isalnum():
        raise ValueError("invalid cohort")
    print(export(args.cohort))
