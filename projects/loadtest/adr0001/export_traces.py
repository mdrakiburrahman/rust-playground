"""Allowlisted projection of finite local traces; never copies runtime configuration."""
import argparse
from collections import defaultdict
import hashlib
import html
from itertools import chain
import json
from pathlib import Path
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
            bundle["coverage"]["dropped_events"] += 1
        for component in sample.get("containers", []):
            source = component["id"]
            process = f"{run_id}-cgroup-{hashlib.sha256(source.encode()).hexdigest()[:16]}"
            at = component["engine_read_ns"]
            origin = origins.setdefault(source, at)
            if at < origin:
                raise ValueError("resource clock moved backwards")
            def add(metric, unit, value, offset, duration):
                if value is None:
                    bundle["coverage"]["unobserved_stages"].append(f"{run_id}-process-{metric}")
                    return
                bundle["records"].append({
                    "id": f"resource-{len(bundle['records'])}", "run_id": run_id,
                    "kind": "resource", "evidence": "measured", "process_id": process,
                    "clock_id": process + "-engine", "metric": metric, "unit": unit,
                    "value": value, "offset_ns": str(offset), "duration_ns": str(duration),
                    "method": "process-counter", "assumption": "process-not-request",
                })
            add("memory", "bytes", component.get("memory_usage_bytes"), at - origin, 0)
            old = previous.get(source)
            if old is not None:
                duration = at - old["engine_read_ns"]
                used = component["cpu_total_usage_ns"] - old["cpu_total_usage_ns"]
                if duration <= 0 or used < 0:
                    raise ValueError("invalid resource counter interval")
                add("cpu", "ns", used, old["engine_read_ns"] - origin, duration)
            previous[source] = component


def export(cohort):
    directory = REPO / "target/delta-benchmark" / cohort
    attempts = bounded_json(directory / "attempts.json")
    bundle = {
        "format": "arrowcol-benchmark", "schema_version": 1, "dataset_id": cohort,
        "provenance": {"source_revision": bounded_json(directory / "revision.json")["commit"],
                       "synthetic": True, "description": "finite-local-comparison"},
        "runs": [], "records": BoundedList(100000), "links": BoundedList(200000),
        "coverage": {"dropped_events": 0, "incomplete_attempts": 0, "complete": True,
                     "unobserved_stages": ["one-way-network", "sql-engine-cpu", "per-request-cpu"]},
    }
    revision = bounded_json(directory / "revision.json")
    if "source_binary_manifest_sha256" in revision:
        manifest_path = directory / "source-binary-sha256.json"
        manifest_hash = hashlib.sha256(bounded_bytes(manifest_path)).hexdigest()
        if manifest_hash != revision["source_binary_manifest_sha256"]:
            raise ValueError("source/binary provenance manifest changed")
        bundle["provenance"]["artifact_sha256"] = [manifest_hash]
    elif revision.get("dirty"):
        raise ValueError("dirty measurement lacks source/binary manifest")
    stages = defaultdict(list)
    for index, attempt in enumerate(attempts):
        root = REPO / attempt["evidence"]
        inputs = bounded_json(root / "benchmark-inputs.json")
        traces = {}
        observed = defaultdict(set)
        operation_units = {}
        run_id = f"r{index}"
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
            units = {}
            commits = {}
            for receipt_index, receipt in enumerate(record["receipts"]):
                receipt_id = f"receipt-{index}-{record_index}-{receipt_index}"
                operation = receipt["operation"]
                if operation not in units:
                    unit_id = f"unit-{index}-{record_index}-{len(units)}"
                    units[operation] = unit_id
                    bundle["records"].append({"id": unit_id, "run_id": run_id, "kind": "unit", "evidence": "measured"})
                    bundle["links"].append({"from": batch_id, "to": unit_id, "relation": "contains"})
                unit_id = units[operation]
                operation_units[operation] = unit_id
                final = receipt["kind"] in ("DELTA_COMMITTED", "DELTA_RESOLVED") and receipt.get("final", True)
                bundle["records"].append({"id": receipt_id, "run_id": run_id, "kind": "receipt", "evidence": "measured",
                                          "status": "completed" if final else "pending"})
                bundle["links"].append({"from": unit_id, "to": receipt_id, "relation": "receipt"})
                if final:
                    identity = (operation, receipt["version"], receipt["digest"])
                    if identity not in commits:
                        commit_id = f"commit-{index}-{record_index}-{len(commits)}"
                        commits[identity] = commit_id
                        bundle["records"].append({"id": commit_id, "run_id": run_id, "kind": "commit", "evidence": "measured",
                                                  "version": receipt["version"], "digest": receipt["digest"],
                                                  "table_id": f"{run_id}-{record['table']}"})
                        bundle["links"].append({"from": unit_id, "to": commit_id, "relation": "publishes"})
                    commit_id = commits[identity]
                    bundle["links"].append({"from": commit_id, "to": receipt_id, "relation": "receipt"})
            duration = round(record["terminal_ms"] * 1_000_000)
            bundle["records"].append({
                "id": f"client-{index}-{record_index}", "run_id": run_id, "kind": "span", "evidence": "measured",
                "trace_id": attempt_id, "role": "client", "process_id": f"client-{index}", "clock_id": f"client-{index}-{record_index}",
                "stage": "rpc-terminal", "category": "transport_observation", "offset_ns": "0", "duration_ns": str(duration),
                "status": "completed" if record["terminal"] == "final" else "unknown", "method": "local-monotonic",
            })
            client_clock = f"client-{index}-{record_index}"
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
                    "role": "client", "process_id": f"client-{index}", "clock_id": client_clock,
                    "stage": stage, "category": "transport_observation", "offset_ns": "0",
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
                    "role": "client", "process_id": f"client-{index}", "clock_id": client_clock,
                    "stage": "ordinary-reader-scan", "category": "downstream_wait",
                    "offset_ns": str(end - scan), "duration_ns": str(scan), "status": "completed",
                    "method": "local-monotonic",
                })
            if record["completion"] == "unresolved":
                bundle["coverage"]["incomplete_attempts"] += 1
        receipts_path = root / "sql-final-receipts.json"
        if receipts_path.exists():
            for receipt in bounded_json(receipts_path):
                operation = receipt["operation_id"]
                if not operation.endswith("-schema"):
                    continue
                unit = operation_units.get(operation.removesuffix("-schema"))
                if unit is None:
                    raise ValueError("unattributed schema commit")
                commit_id, receipt_id = f"schema-{len(bundle['records'])}", f"sql-receipt-{len(bundle['records'])}"
                bundle["records"].extend([
                    {"id": commit_id, "run_id": run_id, "kind": "commit", "evidence": "measured",
                     "version": receipt["version"], "digest": receipt["digest"], "stage": "schema-evolution",
                     "table_id": f"{run_id}-{receipt['table_id']}"},
                    {"id": receipt_id, "run_id": run_id, "kind": "receipt", "evidence": "measured", "status": "completed"},
                ])
                bundle["links"].extend([
                    {"from": unit, "to": commit_id, "relation": "publishes"},
                    {"from": commit_id, "to": receipt_id, "relation": "receipt"},
                ])
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
                trace = traces.get(raw.get("trace_id"))
                if not trace or raw.get("event") != "service_span" or raw["id"] in seen:
                    continue
                seen.add(raw["id"])
                run_id, attempt_id = trace
                item = {key: raw[key] for key in ("role", "stage", "category", "status", "offset_ns", "duration_ns")}
                process = f"process-{index}-{hashlib.sha256(raw['process_id'].encode()).hexdigest()[:16]}"
                if not raw.get("clock_id"):
                    raise ValueError("service span lacks clock identity")
                clock = f"clock-{index}-{hashlib.sha256(raw['clock_id'].encode()).hexdigest()[:16]}"
                item.update(id=f"span-{index}-{len(seen)}", run_id=run_id, kind="span", evidence="measured",
                            trace_id=attempt_id, process_id=process, clock_id=clock, method="local-monotonic")
                bundle["records"].append(item)
                raw_spans[raw["id"]] = item
                if raw.get("parent_id"):
                    parent_refs.append((raw["parent_id"], item))
                bundle["links"].append({"from": attempt_id, "to": item["id"], "relation": "contains"})
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
                bundle["coverage"]["incomplete_attempts"] += 1
                bundle["coverage"]["unobserved_stages"].extend(
                    f"{run_id}-{role}-{stage}" for role, stage in missing)
        resource_records(bundle, root, run_id)
    bundle["coverage"]["unobserved_stages"] = sorted(set(bundle["coverage"]["unobserved_stages"]))
    bundle["coverage"]["complete"] = not (bundle["coverage"]["dropped_events"] or bundle["coverage"]["incomplete_attempts"])
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
        "processing. Resource CPU records are cgroup counter deltas over Engine timestamp intervals; "
        "zero-duration memory records are point samples. Their clocks cannot be subtracted from service clocks. "
        "Dropped-event counts include observed overflow markers (a lower bound on suppressed events).</p>"
        "<p>Client milestone spans overlap: each runs from request start to its named observed event. "
        "RPC-terminal status is separate from the attempt's reconciled completion status. Ordinary-reader "
        "scans are separate post-completion intervals, not exact first visibility.</p>"
        "<table><tr><th>Arm</th><th>Role</th><th>Stage</th><th>Samples</th><th>Mean ms</th></tr>" + rows + "</table>"
        "<p>Failures and unknown outcomes remain in the JSON. Small cohorts do not establish production capacity.</p>"
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
