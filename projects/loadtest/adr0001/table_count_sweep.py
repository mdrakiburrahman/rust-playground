"""Exploratory 1/2/8 actual-TableKey sweep using explicitly frozen SQL/Delta code."""
import argparse
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
import csv
import hashlib
import json
import os
from pathlib import Path
import signal
import statistics
import stat
import subprocess
import sys
import time
import traceback


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False))


def table_specs(count, fingerprint):
    if count not in (1, 2, 8):
        raise ValueError("only the qualified finite 1/2/8 sweep is supported")
    return [{"key": f"table{i:02d}", "path": ["telemetry", "runtime", f"table{i:02d}"],
             "fingerprints": [fingerprint]} for i in range(count)]


def configure_tables(runtime, count):
    runtime.config["tables"] = table_specs(count, runtime.fingerprints[0])
    for path in (runtime.root / "config").glob("*.json"):
        value = json.loads(path.read_text())
        if "tables" in value:
            value["tables"] = runtime.config["tables"]
            mode = stat.S_IMODE(path.stat().st_mode)
            path.chmod(mode | stat.S_IWUSR)
            try:
                save(path, value)
            finally:
                path.chmod(mode)


def distribution(values):
    return {"n": len(values), "p50_ms": statistics.median(values) if values else None,
            "min_ms": min(values) if values else None, "max_ms": max(values) if values else None,
            "p95_ms": None, "p99_ms": None}


def summary(records, seconds):
    complete = [r for r in records if r["completion"] != "unresolved"]
    rpc = [r["terminal_ms"] for r in records if r["terminal"] == "final"]
    physical = [r.get("final_receipt_ms", r.get("resolved_receipt_ms")) for r in complete]
    return {"requests": len(records), "completed": len(complete),
            "rpc_outcomes": dict(Counter(r["terminal"] for r in records)),
            "rows": sum(r["rows"] for r in complete), "offered_phase_seconds": seconds,
            "rows_per_second": sum(r["rows"] for r in complete) / seconds,
            "logical_payload_MiB_per_second": sum(r["rows"] * 8 for r in complete) / seconds / 1048576,
            "rpc_final_elapsed": distribution(rpc),
            "physical_completion_receipt_observed": distribution([v for v in physical if v is not None]),
            "postcompletion_reader_observed": distribution(
                [r["reader_visibility_ms"] for r in complete if r.get("reader_visibility_ms") is not None])}


def cleanup(root):
    ownership = root / "sweep-owned.json"
    if not ownership.exists():
        return []
    records = json.loads(ownership.read_text())
    def stop(record):
        state = json.loads(subprocess.check_output(["docker", "inspect", record["id"]], timeout=5))[0]
        if (state["Id"] != record["id"] or state["State"]["StartedAt"] != record["started"]
                or state["Image"] != record["image"]
                or not any(state["Config"]["Labels"].get(k) == root.name
                           for k in ("delta-runtime", "publication-gate"))):
            raise RuntimeError("exact cleanup ownership mismatch")
        if state["State"].get("Paused"):
            subprocess.run(["docker", "unpause", record["id"]], check=True, capture_output=True, timeout=5)
        subprocess.run(["docker", "stop", "-t", "3", record["id"]], check=True, capture_output=True, timeout=12)
        after = json.loads(subprocess.check_output(["docker", "inspect", record["id"]], timeout=5))[0]
        if after["State"]["Running"]:
            raise RuntimeError("exact resource still running")
        return {"role": record["role"], "stopped": True}
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(stop, records))


def run_case(args, provenance, cohort):
    from delta_benchmark import Benchmark, BINARIES
    from publication_gate import REPO
    arm, count = args.case.split(":")
    count = int(count)
    class Sweep(Benchmark):
        def start_role(self, role, binary, *positional, **keywords):
            if not getattr(self, "sweep_configured", False):
                configure_tables(self, count)
                self.sweep_configured = True
            return super().start_role(role, binary, *positional, **keywords)

        def create_container(self, command):
            identity = super().create_container(command)
            state = json.loads(subprocess.check_output(["docker", "inspect", identity], timeout=5))[0]
            self.sweep_owned = getattr(self, "sweep_owned", [])
            self.sweep_owned.append({"id": identity, "started": state["State"]["StartedAt"],
                                     "image": state["Image"], "role": state["Name"].removeprefix("/" + self.project + "-")})
            save(self.root / "sweep-owned.json", self.sweep_owned)
            return identity

    runtime = Sweep(args.project, args.subnet, arm, 1, cohort / "bin", trace_enabled=True)
    result = {"arm": arm, "table_count": count, "status": "incomplete", "network_only": None,
              "failure_type": None, "cleanup": [], "stages": [], "trace_overflow_markers": 0}
    try:
        runtime.setup_runtime()
        resources = []
        for record in runtime.sweep_owned:
            state = json.loads(subprocess.check_output(["docker", "inspect", record["id"]], timeout=5))[0]
            resources.append({"role": record["role"], "image_id": state["Image"],
                              "nano_cpus": state["HostConfig"]["NanoCpus"],
                              "memory_bytes": state["HostConfig"]["Memory"],
                              "pids_limit": state["HostConfig"]["PidsLimit"],
                              "read_only_root": state["HostConfig"]["ReadonlyRootfs"],
                              "running_at_start": state["State"]["Running"]})
        result["resources"] = resources
        keys = [item["key"] for item in runtime.config["tables"]]
        marker = 0
        for phase, jobs in (("warmup", 8), ("timed", 24)):
            inputs = []
            for index in range(jobs):
                inputs.append((marker, 4096, keys[index % count], phase, runtime.gateway_port))
                marker += 4096
            before = time.monotonic_ns()
            with ThreadPoolExecutor(max_workers=2) as pool:
                records = list(pool.map(runtime.put, inputs))
            elapsed = (time.monotonic_ns() - before) / 1e9
            runtime.records.extend(records)
            save(runtime.root / "benchmark-inputs.json", runtime.records)
            if phase == "timed":
                result.update(summary(records, elapsed))
            if any(r["completion"] == "unresolved" for r in records):
                raise RuntimeError("unresolved request; retained without payload replay")
        scan = subprocess.run([sys.executable, "-B", str(Path(sys.modules["delta_benchmark"].__file__)),
                               "--scan", str(runtime.root)], capture_output=True, text=True, timeout=30)
        if scan.returncode != 0:
            raise RuntimeError("ordinary Delta multiset mismatch")
        scanned = json.loads(scan.stdout)
        if set(scanned) != set(keys) or any(row["missing_submitted_rows"] for row in scanned.values()):
            raise RuntimeError("actual written table cardinality mismatch")
        receipts = json.loads(runtime.sql_query(
            "SET NOCOUNT ON; SELECT table_id,operation_id,version,digest FROM delta_authority.receipts FOR JSON PATH;"))
        by_unit = {r["operation_id"]: r for r in receipts}
        for record in runtime.records:
            final = record["receipts"][-1]
            receipt = by_unit[final["operation"]]
            if any(receipt[k] != expected for k, expected in (
                    ("table_id", record["table"]), ("version", final["version"]), ("digest", final["digest"]))):
                raise RuntimeError("exact-unit SQL receipt mismatch")
        if len(receipts) != 32 + count:
            raise RuntimeError("unattributed SQL receipt")
        preparations = len(list((runtime.root / "delta-private/work").iterdir()))
        if preparations > 64:
            raise RuntimeError("preparation bound exceeded")
        result.update(status="completed", actual_written_tables=keys, ordinary_delta=scanned,
                      verified_data_receipts=32, verified_schema_receipts=count,
                      preparation_attempts=preparations,
                      timed_requests_per_table=dict(Counter(
                          r["table"] for r in runtime.records if r["phase"] == "timed")))
        for binary in BINARIES:
            if digest(runtime.root / "bin" / binary) != provenance["shared_binaries"][binary]:
                raise RuntimeError("executed shared binary changed")
    except Exception as error:
        result.update(status="failed", failure_type=type(error).__name__)
        result["failure_frames"] = [{"file": Path(f.filename).name, "line": f.lineno, "function": f.name}
                                    for f in traceback.extract_tb(error.__traceback__)]
        print("case failed: " + type(error).__name__, flush=True)
    finally:
        stages = defaultdict(list)
        timed = {r["trace_id"] for r in runtime.records if r["phase"] == "timed"}
        try:
            for role, name in runtime.names.items():
                logs = subprocess.run(["docker", "logs", runtime.owned_container(name)],
                                      capture_output=True, timeout=10)
                if logs.returncode != 0 or len(logs.stdout) + len(logs.stderr) > 32 * 1048576:
                    raise RuntimeError("bounded trace log unavailable")
                raw = logs.stdout + logs.stderr
                (runtime.root / "logs" / (role + "-sweep.log")).write_bytes(raw)
                for line in raw.splitlines():
                    if not line.startswith(b"{"):
                        continue
                    try:
                        span = json.loads(line)
                    except json.JSONDecodeError:
                        result["trace_overflow_markers"] += 1
                        continue
                    if span.get("event") == "trace_overflow":
                        result["trace_overflow_markers"] += 1
                    if (span.get("event") == "service_span" and span.get("trace_id") in timed
                            and span.get("role") == "gateway"):
                        stages[(span["stage"], span["status"])].append(int(span["duration_ns"]) / 1e6)
            result["stages"] = [{"stage": stage, "status": status, **distribution(values)}
                                for (stage, status), values in sorted(stages.items())]
        except Exception as error:
            result["trace_error_type"] = type(error).__name__
        try:
            runtime.executor.shutdown(wait=True)
            result["cleanup"] = cleanup(runtime.root)
        except Exception as error:
            result.update(status="failed", cleanup_error_type=type(error).__name__)
        save(runtime.root / "sweep-case.json", result)
    return result["status"] == "completed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--cohort", required=True)
    parser.add_argument("--frozen-cohort", default="eec969a5-q20261030b")
    parser.add_argument("--subnet-start", type=int, default=201)
    parser.add_argument("--case", choices=[f"{a}:{n}" for n in (1, 2, 8) for a in ("pingora", "adr")])
    parser.add_argument("--project")
    parser.add_argument("--subnet")
    args = parser.parse_args()
    if not args.cohort.replace("-", "").isalnum() or len(args.cohort) > 20:
        raise ValueError("unsafe bounded cohort")
    frozen = args.repo / "target/delta-benchmark" / args.frozen_cohort
    provenance = json.loads((frozen / "public/provenance-calibration.json").read_text())
    manifest = frozen / "source-binary-sha256.json"
    if digest(manifest) != provenance["source_binary_manifest_sha256"]:
        raise RuntimeError("frozen manifest changed")
    hashes = json.loads(manifest.read_text())
    for path in ("delta_benchmark.py", "delta_runtime.py", "publication_gate.py", "observation.py"):
        relative = "projects/loadtest/adr0001/" + path
        if digest(args.repo / relative) != hashes[relative]:
            raise RuntimeError("frozen native harness changed")
    for name, value in provenance["shared_binaries"].items():
        if digest(frozen / "bin" / name) != value or digest(args.repo / "target/debug" / name) != value:
            raise RuntimeError("frozen native binary changed")
    os.environ.update(ARROWCOL_SQL_IMAGE="sha256:6fc4994422c0c1b0e0d1a7dc9cbb6ffc035c93d6e7d29b495b88ef71c7ecdf87",
                      ARROWCOL_RUNTIME_IMAGE="ghcr.io/mdrakiburrahman/rust-playground/devcontainer@sha256:c3c7c0aabf082facf709fe2b5f7a2d5cd499a83364463492057268901cb6bede")
    os.umask(0o022)
    sys.path.insert(0, str(args.repo / "projects/loadtest/adr0001"))
    if args.case:
        signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError("case deadline")))
        return 0 if run_case(args, provenance, frozen) else 1
    output = args.repo / "target/table-count-sweep" / args.cohort
    output.mkdir(parents=True, exist_ok=False)
    save(output / "method.json", {"driver_sha256": digest(Path(__file__)),
         "frozen_manifest_sha256": digest(manifest), "shared_binaries": provenance["shared_binaries"],
         "rows_per_request": 4096, "concurrency": 2, "warmup": 8, "timed_requests": 24,
         "tracing": True, "cgroup_sampling": False, "case_timeout_seconds": 180,
         "network_only": None, "command": sys.argv, "sql_image": os.environ["ARROWCOL_SQL_IMAGE"],
         "runtime_image": os.environ["ARROWCOL_RUNTIME_IMAGE"]})
    cases = []
    active = None
    def interrupt(*_):
        if active is not None and active.poll() is None:
            active.terminate()
            try:
                active.wait(timeout=30)
            except subprocess.TimeoutExpired:
                active.kill()
                active.wait(timeout=10)
        raise InterruptedError("overall sweep deadline")
    signal.signal(signal.SIGTERM, interrupt)
    for index, (count, arm) in enumerate(((1, "pingora"), (1, "adr"), (2, "adr"),
                                         (2, "pingora"), (8, "pingora"), (8, "adr"))):
        project = f"publication-gate-{args.cohort}-{index}"
        root = args.repo / "target/publication-gate" / project
        command = [sys.executable, "-B", str(Path(__file__)), "--repo", str(args.repo),
                   "--cohort", args.cohort, "--frozen-cohort", args.frozen_cohort,
                   "--case", f"{arm}:{count}", "--project", project,
                   "--subnet", f"10.235.{args.subnet_start + index}.0/24"]
        log = (output / f"case-{index}.log").open("xb")
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        active = process
        try:
            code = process.wait(timeout=180)
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                code = process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                code = process.wait(timeout=10)
        finally:
            log.close()
            stopped = cleanup(root)
            active = None
        path = root / "sweep-case.json"
        result = json.loads(path.read_text()) if path.exists() else {
            "arm": arm, "table_count": count, "status": "failed", "failure_type": "MissingCaseResult"}
        result.update(process_exit_code=code, independently_verified_cleanup=stopped)
        cases.append(result)
        save(output / "results.json", cases)
        print(json.dumps({"arm": arm, "tables": count, "status": result["status"],
                          "rows_per_second": result.get("rows_per_second")}), flush=True)
        if code != 0 or result["status"] != "completed":
            return 1
    for name, value in provenance["shared_binaries"].items():
        if digest(args.repo / "target/debug" / name) != value:
            raise RuntimeError("shared binary changed after execution")
    columns = ["arm", "table_count", "status", "requests", "completed", "rows", "offered_phase_seconds",
               "rows_per_second", "logical_payload_MiB_per_second"]
    with (output / "results.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cases)
    return 0


if __name__ == "__main__":
    sys.exit(main())
