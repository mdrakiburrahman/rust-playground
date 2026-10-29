"""Bounded matched generated input and architecture-specific authoritative output accounting."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import threading
import time
import uuid

import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "routing-benchmark"))
import native

# Avoid collision with the unchanged baseline's `stack` module.
spec = importlib.util.spec_from_file_location("adr_stack", Path(__file__).with_name("stack.py"))
adr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(adr)
REPO = Path(__file__).resolve().parents[3]


def sdks(baseline):
    root = Path(baseline) / "prepared"
    exports = json.loads((root / "results/prepared-tables.json").read_text())
    return [native.load_sdk(root / row["sdk_root"]) for row in exports]


def baseline_module(root):
    path = Path(root).resolve().parents[2] / "projects/loadtest/routing-benchmark/stack.py"
    spec = importlib.util.spec_from_file_location("original_baseline_stack", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def table_key(manifest):
    identity = manifest["table"]
    # Existing immutable catalog ID, scoped by the authenticated single tenant/catalog configuration.
    # Schema fingerprint/version never enters ownership identity.
    return identity["table_id"]


def catalog(baseline):
    return [{"key": table_key(m), "path": [m["table"][k] for k in ("catalog", "schema", "table")],
             "fingerprints": [m["table"]["fingerprint"]]} for m, _ in sdks(baseline)]


def make_retained_batch(schema, rows, marker):
    if not any(pa.types.is_dictionary(field.type) for field in schema):
        return native.make_batch(schema, rows, marker)
    logical = pa.schema([pa.field(field.name, field.type.value_type if pa.types.is_dictionary(field.type)
                                  else field.type, nullable=field.nullable, metadata=field.metadata)
                         for field in schema], metadata=schema.metadata)
    batch = native.make_batch(logical, rows, marker)
    arrays = [pa.array(column.to_pylist(), type=field.type) if pa.types.is_dictionary(field.type) else column
              for field, column in zip(schema, batch.columns)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


class UploadResult(dict):
    """JSON evidence stays primitive; caller owns actual unresolved Arrow buffers."""
    retry_handle = None
    retry_handles = ()


class RetryHandle:
    def __init__(self, sdk, settings, ordinal, batches):
        self.sdk = (json.loads(json.dumps(sdk[0])), sdk[1])
        self.settings, self.ordinal = dict(settings), ordinal
        self.batches = tuple(batches)

    def resume(self, endpoint, secret, max_attempts=3):
        return retained_upload(endpoint, secret, self.sdk,
                               dict(self.settings, max_attempts=max_attempts), self.ordinal,
                               retry_handle=self)


def retained_upload(endpoint, secret, sdk, settings, ordinal, lose_first_reply=False, accounting=None,
                    retry_handle=None, batch_observer=None):
    if not 1 <= settings.get("max_attempts", 1) <= 8:
        raise ValueError("finite retry attempt bound: 1..8")
    if not 1 <= settings["rows"] <= 65536 or not 1 <= settings["batches_per_stream"] <= 64:
        raise ValueError("finite retained input dimensions")
    if accounting is not None:
        accounting.begin(ordinal)
    manifest, schema = sdk
    identity = manifest["table"]
    marker = f"{settings['run_id']}/{ordinal}"
    batches = retry_handle.batches if retry_handle is not None else []
    if retry_handle is None:
        retained = 0
        for i in range(settings["batches_per_stream"]):
            batch = make_retained_batch(schema, settings["rows"], f"{marker}/{i}")
            retained += batch.nbytes
            if batch.nbytes > 2 * 1024 * 1024 or retained > 8 * 1024 * 1024:
                raise ValueError("retained complete Arrow unit bound")
            batches.append(batch)
    if sum(batch.nbytes for batch in batches) > 8 * 1024 * 1024:
        raise ValueError("retained complete Arrow unit bound")
    expected = [f"{marker}/{b}/{r}" for b in range(len(batches)) for r in range(settings["rows"])]
    outcome = UploadResult({"ordinal": ordinal, "table": table_key(manifest), "markers": expected,
               "generated_rows": len(expected), "arrow_bytes": sum(b.nbytes for b in batches),
               "retained_bytes": sum(b.nbytes for b in batches), "attempts": [], "receipts": [],
               "terminal": "unresolved"})
    if accounting is not None:
        accounting.retain(ordinal, outcome["arrow_bytes"])
    start = time.monotonic()
    for attempt in range(1, settings.get("max_attempts", 1) + 1):
        attempt_start = time.monotonic()
        record = {"number": attempt, "receipts": [], "error": None,
                  "admission_ms": None, "admission_semantics": "unavailable: no provisional admission receipt"}
        client = flight.FlightClient(endpoint, write_size_limit_bytes=1024 * 1024)
        writer = None
        if accounting is not None:
            accounting.attempt(ordinal, True)
        try:
            headers = {
                "content-type": "application/grpc", "x-arrowcol-protocol-version": "1",
                "x-arrowcol-implementation-pool": "direct",
                "x-arrowcol-catalog": identity["catalog"], "x-arrowcol-schema": identity["schema"],
                "x-arrowcol-table": identity["table"], "x-arrowcol-schema-fingerprint": identity["fingerprint"],
            }
            headers = native.signed_headers(secret, "POST", native.PUT_PATH, headers)
            options = flight.FlightCallOptions(timeout=15, headers=[(k.encode(), v.encode()) for k, v in headers.items()])
            writer, reader = client.do_put(flight.FlightDescriptor.for_path(*manifest["flight"]["descriptor_path"]), schema, options)
            record["stream_open_ms"] = (time.monotonic() - attempt_start) * 1000
            for batch_index, batch in enumerate(batches):
                writer.write_batch(batch)
                raw = reader.read()
                if raw is None:
                    # PyArrow may expose terminal gRPC status on the writer rather than metadata reader.
                    # Do not misclassify a permanent auth/schema failure as an uncertain empty receipt.
                    writer.close()
                    writer = None
                    raise RuntimeError("ended without TABLE_COMMITTED")
                receipt = json.loads(raw.to_pybytes())
                if receipt.get("kind") != "TABLE_COMMITTED":
                    raise RuntimeError("provisional receipt is not success")
                receipt["observed_ms"] = (time.monotonic() - start) * 1000
                record["receipts"].append(receipt)
                # Deterministic lost-observation injection, after server publication but before application success.
                if lose_first_reply and attempt == 1:
                    raise ConnectionError("injected loss of final reply observation; complete unit retained")
                outcome["receipts"].append(receipt)
                if batch_observer is not None:
                    batch_observer(attempt, batch_index, receipt)
                if settings.get("pacing_ms", 0):
                    time.sleep(settings["pacing_ms"] / 1000)
            writer.done_writing()
            while reader.read() is not None:
                pass
            writer.close()
            writer = None
            outcome["terminal"] = "ok"
            record["terminal"] = "ok"
            break
        except Exception as error:
            record["error"] = type(error).__name__ + ": " + str(error)[:1024]
            record["terminal"] = "uncertain"
            # Permanent authorization/schema failures are not a retry loop.
            if isinstance(error, (flight.FlightUnauthenticatedError, flight.FlightUnauthorizedError, pa.ArrowInvalid)):
                record["terminal"] = "permanent"
                break
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass
            client.close()
            record["terminal_ms"] = (time.monotonic() - attempt_start) * 1000
            outcome["attempts"].append(record)
            if accounting is not None:
                accounting.attempt(ordinal, False)
        if outcome["terminal"] != "ok":
            time.sleep(min(attempt * 0.15, 0.6))
    outcome["elapsed_ms"] = (time.monotonic() - start) * 1000
    outcome["retained_bytes"] = 0 if outcome["terminal"] == "ok" else outcome["retained_bytes"]
    if outcome["terminal"] != "ok":
        outcome.retry_handle = RetryHandle(sdk, settings, ordinal, batches)
        outcome["retention"] = "returned in-process retry_handle owns complete Arrow batches and schema"
    if accounting is not None:
        accounting.complete(ordinal, outcome["terminal"] == "ok")
    return outcome


def visible(root, view=None):
    root = Path(root)
    view = view or adr.control(root, {"op": "view"})
    result = {}
    manifests = []
    for assignment in view["assignments"]:
        record = assignment["record"]
        head = record["head"]
        markers = []
        seen = set()
        while head:
            if head["commit"] in seen or len(seen) >= 100_000:
                raise RuntimeError("manifest history cycle/depth")
            seen.add(head["commit"])
            raw = (root / "objects" / head["manifest"]).read_bytes()
            if hashlib.sha256(raw).hexdigest() != head["sha256"]:
                raise RuntimeError("authoritative manifest digest mismatch")
            manifest = json.loads(raw)
            data = root / "objects" / manifest["parquet"]
            if hashlib.sha256(data.read_bytes()).hexdigest() != manifest["parquet_sha256"]:
                raise RuntimeError("committed Parquet digest mismatch")
            if manifest["table"] != record["key"] or manifest["commit"] != head["commit"]:
                raise RuntimeError("manifest table/commit mismatch")
            table = pq.read_table(data, columns=["sql_server_instance_name"], use_threads=False)
            markers.extend(table.column(0).to_pylist())
            manifests.append({"commit": head["commit"], "manifest_bytes": len(raw),
                              "parquet_bytes": data.stat().st_size, "rows": len(table)})
            head = manifest["parent"]
        result[record["key"]] = markers
    return result, manifests


def resource_snapshot(command):
    ids = subprocess.check_output(command + ["ps", "-q"], text=True).split()
    if not ids:
        return []
    raw = subprocess.check_output(["docker", "--context", "default", "stats", "--no-stream",
                                  "--format", "{{json .}}", *ids], text=True)
    return [json.loads(line) for line in raw.splitlines()]


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), **{f"p{p}": values[min(len(values)-1, int(len(values)*p/100))]
                                      if values else None for p in (50, 95, 99)}}


def run(design, root, baseline, label, indexes=None, streams=12, rows=32, batches=2,
        endpoints=None, max_attempts=1, lose_first_reply=False, cohort=None, observer=None, pacing_ms=0):
    indexes = [0, 1, 2] if indexes is None else indexes
    root = Path(root)
    output = REPO / "target/adr0001/evidence"
    if cohort:
        if not cohort.replace("-", "").isalnum():
            raise ValueError("unsafe cohort")
        output = output / cohort
    output = output / design / label
    output.mkdir(parents=True, exist_ok=False)
    all_sdks = sdks(baseline)
    chosen = [all_sdks[i] for i in indexes]
    settings = {"run_id": hashlib.sha256(label.encode()).hexdigest()[:16], "rows": rows,
                "batches_per_stream": batches, "max_attempts": max_attempts, "streams": streams,
                "workers": 3, "seed": 42, "pacing_ms": pacing_ms,
                "schema_fingerprints": [m["table"]["fingerprint"] for m, _ in chosen]}
    if design == "baseline":
        command = baseline_module(root).context(root)[1]
        endpoints = endpoints or ["grpc://" + subprocess.check_output(command + ["port", "router", "50050"], text=True).strip()]
        settings.update({"edge_secret": (root / "secrets/edge_hmac").read_bytes().rstrip(b"\r\n"),
                         "endpoints": endpoints, "timeout": 30, "stream_seconds": 15,
                         "max_batch_bytes": 1024*1024, "publication_timeout": 60,
                         "receipt_endpoint": "http://" + endpoints[0].split("://")[1]})
    else:
        command = adr.compose(root)
        endpoints = endpoints or [adr.endpoint(root)]
        config = json.loads((root / "secrets/controller.json").read_text())
        secret = config["edge_secret"].encode()
    (output / "settings.json").write_text(json.dumps({k: v for k, v in settings.items() if k != "edge_secret"}, indent=2))
    before = resource_snapshot(command)
    started = time.monotonic()
    interval_started_ns = time.time_ns()
    accounting = observer.phase(label, streams, design) if observer is not None else None
    original_make_batch = native.make_batch
    unit_local = threading.local()
    def baseline_unit(ordinal):
        if accounting is not None:
            accounting.begin(ordinal)
            unit_local.ordinal = ordinal
        outcome = native.upload(settings, chosen[ordinal % len(chosen)], ordinal)
        if accounting is not None:
            accounting.complete(ordinal, outcome["terminal"] == "ok")
        return outcome
    def observe_batch(*args, **kwargs):
        batch = original_make_batch(*args, **kwargs)
        if accounting is not None and hasattr(unit_local, "ordinal"):
            accounting.baseline_batch(unit_local.ordinal, batch.nbytes)
        return batch
    if accounting is not None and design == "baseline":
        native.make_batch = observe_batch
    # Bounded native threads: PyArrow is not assumed gevent cooperative.
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            if design == "baseline":
                outcomes = list(pool.map(baseline_unit, range(streams)))
            else:
                outcomes = list(pool.map(lambda n: retained_upload(endpoints[n % len(endpoints)], secret,
                    chosen[n % len(chosen)], settings, n, lose_first_reply and n == 0, accounting), range(streams)))
    finally:
        native.make_batch = original_make_batch
    terminal_seconds = time.monotonic()-started
    if design == "baseline":
        reconciliation = native.reconcile(settings, outcomes, root / "direct")
        visible_rows = sum(v.get("visible_run_rows", 0) for v in reconciliation["output"].values())
        missing = sum(v.get("missing_rows", 0) for v in reconciliation["output"].values())
        duplicates = sum(v.get("unconfirmed_or_duplicate_rows", 0) for v in reconciliation["output"].values())
        reconciled = reconciliation["reconciled"]
        final_ack_latencies = []
        for outcome in outcomes:
            for receipt in outcome["receipts"]:
                receipt_id = receipt["receipt_id"]
                # Reconciliation completion is a measured upper bound on final receipt observation.
                if receipt_id in reconciliation["published"]:
                    final_ack_latencies.append((time.time_ns()//1_000_000) - outcome["started_at_unix_ms"])
    else:
        actual, manifests = visible(root)
        expected = Counter(marker for o in outcomes if o["terminal"] == "ok" for marker in o["markers"])
        observed = Counter(marker for markers in actual.values() for marker in markers
                           if marker.startswith(settings["run_id"] + "/"))
        visible_rows = sum(observed.values())
        missing = sum((expected-observed).values())
        duplicates = sum((observed-expected).values())
        reconciled = missing == 0 and all(o["terminal"] == "ok" for o in outcomes) and (max_attempts > 1 or duplicates == 0)
        reconciliation = {"manifests": manifests, "expected_rows": sum(expected.values()),
                          "actual_rows": visible_rows, "missing_rows": missing, "duplicates": duplicates}
        final_ack_latencies = [r["observed_ms"] for o in outcomes for r in o["receipts"]]
    elapsed = time.monotonic()-started
    interval_ended_ns = time.time_ns()
    if observer is not None:
        observer.finish(label)
    after = resource_snapshot(command)
    report = UploadResult({"design": design, "label": label, "settings": {k:v for k,v in settings.items() if k != "edge_secret"},
              "elapsed_seconds": elapsed, "terminal_seconds": terminal_seconds,
              "generated_rows": sum(o["generated_rows"] for o in outcomes),
              "arrow_bytes": sum(o["arrow_bytes"] for o in outcomes), "visible_rows": visible_rows,
              "missing_rows": missing, "duplicates_or_unconfirmed": duplicates, "reconciled": reconciled,
              "unresolved_streams": sum(o["terminal"] != "ok" for o in outcomes),
              "retries": sum(max(0, len(o.get("attempts", []))-1) for o in outcomes),
              "retained_bytes": sum(o.get("retained_bytes", 0) for o in outcomes),
              "retained_input_upper_bound_bytes": streams*max((o["arrow_bytes"] for o in outcomes),default=0),
              "stream_open_latency_ms": distribution([a["stream_open_ms"] for o in outcomes
                  for a in o.get("attempts", []) if "stream_open_ms" in a]),
              "admission_latency_ms": distribution([]),
              "admission_semantics": ("unavailable: baseline client did not timestamp provisional admission"
                  if design == "baseline" else "unavailable: ADR only emits final TABLE_COMMITTED"),
              "terminal_latency_ms": distribution([o["elapsed_ms"] for o in outcomes]),
              "final_commit_observation_ms": distribution(final_ack_latencies),
              "baseline_final_commit_observation_is_poll_upper_bound": design == "baseline",
              "visible_rows_per_second": visible_rows/elapsed,
              "resources_before": before, "resources_after": after,
              "generator": {"threads": 3, "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss},
              "reconciliation": reconciliation})
    report.retry_handles = tuple(o.retry_handle for o in outcomes
                                if isinstance(o, UploadResult) and o.retry_handle is not None)
    if observer is not None:
        report["observation"] = {"started_ns": interval_started_ns, "ended_ns": interval_ended_ns,
            "directory": str(observer.directory.relative_to(REPO)), "client_final": accounting.sample(),
            "cohort": cohort, "pacing_ms": pacing_ms}
    (output / "streams.json").write_text(json.dumps(outcomes, indent=2))
    (output / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({k: report[k] for k in ("design", "label", "reconciled", "visible_rows", "missing_rows", "unresolved_streams")}), flush=True)
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("design", choices=["baseline", "adr"])
    parser.add_argument("--root", required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    raise SystemExit(0 if run(args.design, args.root, args.baseline, args.label)["reconciled"] else 1)
