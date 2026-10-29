"""Finite native pilot and shared settings for Locust."""

import argparse
import json
import os
import resource
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

from native import load_sdk, reconcile, upload


def settings_from_file(path):
    settings = json.loads(Path(path).read_text())
    bounds = {
        "workers": (1, 16), "rows": (1, 8192), "streams": (1, 4096),
        "batches_per_stream": (1, 64), "timeout": (1, 120),
        "stream_seconds": (1, 60), "pacing_ms": (0, 1000),
        "max_batch_bytes": (1024, 8 * 1024 * 1024), "publication_timeout": (1, 300),
        "duration_seconds": (1, 300),
    }
    for key, (minimum, maximum) in bounds.items():
        value = settings.get(key)
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"{key} must be in {minimum}..={maximum}")
    if settings["stream_seconds"] >= settings["timeout"]:
        raise ValueError("stream lifetime must be shorter than RPC deadline")
    if settings["workers"] * settings["max_batch_bytes"] > 64 * 1024 * 1024:
        raise ValueError("outstanding logical Arrow bytes exceed the 64 MiB pilot budget")
    if settings["streams"] * settings["batches_per_stream"] * settings["rows"] > 2_000_000:
        raise ValueError("pilot generation exceeds two million rows")
    if not settings["endpoints"] or len(settings["endpoints"]) > 2:
        raise ValueError("one or two router endpoints are required")
    if not settings["run_id"].replace("-", "").replace("_", "").isalnum():
        raise ValueError("run_id must be a safe unique component")
    secret = Path(settings["edge_secret_file"]).read_bytes().rstrip(b"\r\n")
    if len(secret) < 32:
        raise ValueError("edge secret is too short")
    settings["edge_secret"] = secret
    exports = json.loads(Path(settings["prepared_tables"]).read_text())
    selected = set(settings.get("tables", []))
    sdks = [load_sdk(Path(settings["sdk_base"]) / row["sdk_root"]) for row in exports if not selected or row["identity"] in selected]
    if not sdks or len(sdks) > 256:
        raise ValueError("finite pilot requires 1..=256 prepared tables")
    return settings, sdks


def save_report(settings, outcomes, started):
    output = Path(settings["results"])
    output.mkdir(parents=True, exist_ok=True)
    with (output / "streams.jsonl").open("w") as stream:
        for outcome in outcomes:
            stream.write(json.dumps(outcome) + "\n")
    report = reconcile(settings, outcomes, settings["delta_root"])
    report.update({
        "run_id": settings["run_id"], "elapsed_seconds": time.monotonic() - started,
        "first_stream_started_at_unix_ms": min(
            (outcome["started_at_unix_ms"] for outcome in outcomes), default=None
        ),
        "reconciliation_finished_at_unix_ms": time.time_ns() // 1_000_000,
        "settings": {k: v for k, v in settings.items() if k != "edge_secret"},
        "generation": {
            "streams": len(outcomes),
            "generated_rows": sum(o["generated_rows"] for o in outcomes),
            "write_completed_batches": sum(o["write_completed_batches"] for o in outcomes),
            "arrow_bytes": sum(o["arrow_bytes"] for o in outcomes),
            "terminal_ok": sum(o["terminal"] == "ok" for o in outcomes),
            "not_started_streams": settings["streams"] - len(outcomes),
        },
        "host": {"logical_cpus": os.cpu_count(), "physical_capacity_added_claim": False},
    })
    latencies = sorted(outcome["elapsed_ms"] for outcome in outcomes)
    report["terminal_latency_ms"] = {
        f"p{percentile}": latencies[min(len(latencies) - 1, int(len(latencies) * percentile / 100))]
        if latencies else None for percentile in (50, 95, 99)
    }
    elapsed = report["elapsed_seconds"]
    report["generated_rows_per_second"] = report["generation"]["generated_rows"] / elapsed
    report["reconciled_rows_per_second"] = sum(
        table.get("receipt_published_rows", 0) for table in report["output"].values()
    ) / elapsed if report["reconciled"] else None
    usage = resource.getrusage(resource.RUSAGE_SELF)
    report["load_generator"] = {
        "process_lifetime_cpu_seconds": usage.ru_utime + usage.ru_stime,
        "process_lifetime_peak_rss_kib": usage.ru_maxrss,
        "native_worker_limit": settings["workers"],
        "maximum_outstanding_arrow_bytes": settings["workers"] * settings["max_batch_bytes"],
    }
    report["endpoint_streams"] = {
        endpoint: {
            "streams": sum(outcome.get("endpoint") == endpoint for outcome in outcomes),
            "terminal_ok": sum(outcome.get("endpoint") == endpoint and outcome["terminal"] == "ok" for outcome in outcomes),
        }
        for endpoint in settings["endpoints"]
    }
    report["successful_pilot"] = bool(outcomes) and report["reconciled"] and all(
        outcome["terminal"] == "ok" and outcome["generated_batches"] > 0 for outcome in outcomes
    )
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def run(path):
    settings, sdks = settings_from_file(path)
    started = time.monotonic()
    outcomes = []
    # At most one outstanding future per native worker: no unbounded executor queue.
    with ThreadPoolExecutor(max_workers=settings["workers"]) as pool:
        outstanding = {}
        ordinal = 0
        deadline = started + settings["duration_seconds"]
        while (ordinal < settings["streams"] and time.monotonic() < deadline) or outstanding:
            while ordinal < settings["streams"] and len(outstanding) < settings["workers"] and time.monotonic() < deadline:
                future = pool.submit(upload, settings, sdks[ordinal % len(sdks)], ordinal)
                outstanding[future] = ordinal
                ordinal += 1
            if not outstanding:
                break
            completed, _ = wait(outstanding, return_when=FIRST_COMPLETED)
            for future in completed:
                outcomes.append(future.result())
                del outstanding[future]
    report = save_report(settings, outcomes, started)
    print(json.dumps({"reconciled": report["reconciled"], "streams": len(outcomes), "results": settings["results"]}))
    return 0 if report["successful_pilot"] else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("settings")
    raise SystemExit(run(parser.parse_args().settings))
