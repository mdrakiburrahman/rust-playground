"""Finite matched SQL/Delta cohorts; preserves every attempt, never payload-replays.

Uses the qualified Runtime lifecycle and native binaries without rebuilding.
Run with the existing venv. A run uses 112/128 preparation slots, including
no-op schema work, and never purports to measure sustained capacity.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import resource
import shutil
import socket
import subprocess
import sys
import time
import uuid

import pyarrow as pa
import pyarrow.flight as flight

from delta_runtime import Runtime, native
from publication_gate import REPO
from observation import Observer, read_ns


PHASES = [
    ("cold", 2, 1, 4096),
    ("warm", 12, 2, 4096),
    ("resource-pressure", 8, 4, 32768),
    ("elephant-table", 8, 4, 65536),
    ("cooperative-migration", 6, 2, 4096),
    ("collectors-four", 6, 2, 4096),
    ("collectors-three", 4, 2, 4096),
    ("gateways-two", 6, 2, 4096),
    ("gateways-one", 4, 2, 4096),
]
BINARIES = ("adr-controller", "adr-flight", "adr-publisher", "arrowcol-router-rust",
            "delta-contract", "adr-fingerprint", "publication-gate")


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2))


def distribution(values):
    values = sorted(values)
    def percentile(p):
        index = (len(values) - 1) * p
        low = int(index)
        return values[low] + (values[min(low + 1, len(values) - 1)] - values[low]) * (index - low)
    return {"n": len(values), "p50": percentile(.5) if values else None,
            "p95": percentile(.95) if len(values) >= 100 else None,
            "p99": percentile(.99) if len(values) >= 1000 else None,
            "exploratory_p95": percentile(.95) if values else None,
            "min": min(values) if values else None, "max": max(values) if values else None}


class Benchmark(Runtime):
    def __init__(self, project, subnet, arm, repetition, frozen):
        super().__init__(project, subnet, arm, "sql")
        self.repetition = repetition
        self.frozen = frozen
        self.records = []
        self.phases = []
        self.transitions = []
        self.observer = None
        self.gateway_ports = []
        self.next_marker = 0
        shutil.copy2(Path(__file__), self.root / "measured-benchmark.py")

    def start_role(self, role, binary, *args, **kwargs):
        # setup_runtime's initial copies are overwritten before any role starts.
        for name in BINARIES:
            destination = self.root / "bin" / name
            if not destination.exists() or digest(destination) != digest(self.frozen / name):
                shutil.copy2(self.frozen / name, destination)
        return super().start_role(role, binary, *args, **kwargs)

    def setup_benchmark(self):
        self.setup_runtime()
        self.gateway_ports = [self.gateway_port]
        save(self.root / "measured-binaries.json",
             {name: digest(self.root / "bin" / name) for name in BINARIES})
        # SQL uses the lifecycle's publication-gate label, so observe it through
        # the same narrowly scoped selector as the native runtime roles.
        self.observer = Observer(self.project, self.root / "observations", cadence=1,
                                 probe=lambda: self.post({"op": "view"}),
                                 label_namespace="delta-runtime")
        original_sample = self.observer.sample
        # SQL is not relabeled/mutated. Its own cgroup samples are read explicitly.
        self.sql_samples = []
        def sample():
            original_sample()
            raw = self.observer.engine.get(f"/containers/{self.sql_name}/stats?stream=false&one-shot=true")
            self.sql_samples.append({"time_ns": time.time_ns(), "phase": self.observer.current,
                "engine_read_ns": read_ns(raw["read"]),
                "cpu_total_usage_ns": raw["cpu_stats"]["cpu_usage"]["total_usage"],
                "memory_usage_bytes": raw.get("memory_stats", {}).get("usage"),
                "memory_anon_bytes": raw.get("memory_stats", {}).get("stats", {}).get("anon"),
                "blkio": raw.get("blkio_stats", {})})
            save(self.root / "sql-cgroup.json", self.sql_samples)
        self.observer.sample = sample
        self.observer.start()

    def put(self, job):
        first, count, key, phase, port = job
        start = time.monotonic_ns()
        record = {"first": first, "rows": count, "logical_arrow_bytes": count * 8,
                  "table": key, "phase": phase, "gateway_port": port,
                  "started_ns": time.time_ns(), "receipts": [], "terminal": "unknown",
                  "payload_retries": 0, "reader_visibility_ms": None}
        record["trace_id"] = str(uuid.uuid4())
        schema = self.schemas[0]
        batch = pa.RecordBatch.from_arrays([pa.array(range(first, first + count), type=pa.int64())], schema=schema)
        headers = native.signed_headers(self.config["edge_secret"].encode(), "POST", native.PUT_PATH, {
            "content-type": "application/grpc", "x-arrowcol-protocol-version": "1",
            "x-arrowcol-implementation-pool": "direct", "x-arrowcol-catalog": "telemetry",
            "x-arrowcol-schema": "runtime", "x-arrowcol-table": key,
            "x-arrowcol-schema-fingerprint": self.fingerprints[0]})
        headers["x-adr-trace"] = record["trace_id"]
        client = flight.FlightClient(f"grpc://127.0.0.1:{port}")
        writer = None
        try:
            writer, reader = client.do_put(flight.FlightDescriptor.for_path("telemetry", "runtime", key, "v1"),
                schema, flight.FlightCallOptions(timeout=35, headers=[(k.encode(), v.encode()) for k, v in headers.items()]))
            writer.write_batch(batch)
            writer.done_writing()
            while True:
                raw = reader.read()
                if raw is None:
                    raise RuntimeError("stream ended without final receipt")
                receipt = json.loads(raw.to_pybytes())
                at = (time.monotonic_ns() - start) / 1e6
                record["receipts"].append({"observed_ms": at, **receipt})
                if receipt["kind"] == "DELTA_COMMITTED":
                    record["final_receipt_ms"] = at
                    break
                assert receipt["kind"] == "DELTA_OPERATION" and receipt["final"] is False
                record["admission_identity_observed_ms"] = at
            while reader.read() is not None:
                pass
            writer.close()
            writer = None
            record["terminal"] = "final"
            record["terminal_ms"] = (time.monotonic_ns() - start) / 1e6
        except Exception as error:
            record["error"] = str(error)[:1500]
            record["terminal_ms"] = (time.monotonic_ns() - start) / 1e6
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception as error:
                    record["terminal_close_error"] = str(error)[:1500]
            client.close()
        if record["terminal"] == "final":
            record["completion"] = "stream-final"
        elif record["receipts"]:
            operation = record["receipts"][0]["operation"]
            try:
                resolved = self.wait(lambda: self.final(operation, key), seconds=45)
                record["completion"] = "identity-resolved-final"
                record["resolved_receipt_ms"] = (time.monotonic_ns() - start) / 1e6
                record["receipts"].append({"kind": "DELTA_RESOLVED", **resolved})
            except Exception as error:
                record["completion"] = "unresolved"
                record["resolution_error"] = str(error)[:1000]
        else:
            record["completion"] = "unresolved"
        # Reader work is separately timed; included in offered-load wall time,
        # never represented as exact first visibility or internal commit latency.
        if record["completion"] != "unresolved":
            from deltalake import DeltaTable
            scan_start = time.monotonic_ns()
            table = DeltaTable(str(self.root / "delta-private/tables" / key))
            actual = table.to_pyarrow_dataset().to_table(columns=["id"], use_threads=False,
                batch_readahead=0, fragment_readahead=1)["id"].to_pylist()
            assert set(range(first, first + count)).issubset(actual), "final receipt preceded readable markers"
            record["reader_visibility_ms"] = (time.monotonic_ns() - start) / 1e6
            record["reader_scan_ms"] = (time.monotonic_ns() - scan_start) / 1e6
            record["reader_version"] = table.version()
        record["ended_ns"] = time.time_ns()
        return record

    def sql_query(self, query):
        result = subprocess.run(["docker", "exec", "-i", self.sql_name, "bash", "-c",
            "IFS= read -r SQLCMDPASSWORD; export SQLCMDPASSWORD; "
            "exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U delta_runtime "
            "-d delta_runtime -C -b -w 65535 -y 0 -Q " + "'" + query.replace("'", "'\\''") + "'"],
            input=self.runtime_password.read_text(), text=True, capture_output=True, timeout=30)
        assert result.returncode == 0, result.stderr
        # FOR JSON streams 2,033-character SQL result chunks. sqlcmd inserts
        # physical line breaks between chunks; JSON string escapes stay intact.
        return result.stdout.replace("\r", "").replace("\n", "").strip()

    def change_topology(self, phase):
        started = time.monotonic_ns()
        event = {"phase": phase, "started_ns": time.time_ns()}
        if phase == "cooperative-migration":
            before = self.assignment()["record"]
            self.post({"op": "migrate", "key": "main"})
            after = self.wait(lambda: (a if (a := self.assignment())["record"]["lifecycle"] == "Active"
                and a["record"]["epoch"] > before["epoch"] else None))
            assert after["record"]["writer"] != before["writer"]
            event.update(before=before, after=after["record"], manual=True)
        elif phase == "collectors-four":
            self.start_role("collector4", "adr-flight", ["--config", "/config.json", "--collector", "collector4"],
                            aliases=["collector4"], stage=True)
            self.wait(lambda: sum(m["eligible"] for m in self.post({"op": "view"})["members"]) == 4)
        elif phase == "collectors-three":
            state = self.post({"op": "view"})
            member = next(m for m in state["members"] if m["writer"]["collector"] == "collector4")
            revision = state.get("policies", {}).get("collector4", {}).get("revision", 0)
            operation = str(uuid.uuid4())
            container = json.loads(self.run(["docker", "inspect", self.names["collector4"]]).stdout)[0]
            assert container["Config"]["Labels"].get("delta-runtime") == self.project
            container_id = container["Id"]
            self.post({"op": "policy", "collector": "collector4", "writer": member["writer"],
                       "revision": revision, "operation": operation, "excluded": True,
                       "block_streams": False, "retiring": True})
            def retirement_approved():
                policy = self.post({"op": "view"}).get("policies", {}).get("collector4", {})
                return policy if (policy.get("approved") and policy.get("incarnation") == member["writer"]
                    and policy.get("revision") == revision + 1 and policy.get("operation") == operation
                    and not policy.get("recovery_obligations")) else None
            approval = self.wait(retirement_approved)
            event["retirement_policy"] = approval
            current = json.loads(self.run(["docker", "inspect", self.names["collector4"]]).stdout)[0]
            assert current["Id"] == container_id, "collector container identity changed during retirement"
            self.run(["docker", "stop", "-t", "1", container_id])
            self.wait(lambda: sum(m["eligible"] for m in self.post({"op": "view"})["members"]) == 3)
            for key in ("main", "cas"):
                self.wait(lambda key=key: self.assignment(key)["record"]["lifecycle"] == "Active")
        elif phase == "gateways-two":
            scoped = dict(self.config)
            scoped.update(etcd=[], etcd_user="", etcd_password="", operator_token="", collectors={})
            config = self.root / "config/gateway2.json"
            save(config, scoped)
            config.chmod(0o600)
            if self.arm == "adr":
                self.start_role("gateway2", "adr-flight", ["--config", "/config.json"], ports=[50051])
            else:
                self.start_role("gateway2", "arrowcol-router-rust", ports=[50051],
                    env=["ARROWCOL_DELTA_CONFIG=/config.json", "ARROWCOL_DELTA_MODE=data_only"])
            second = self.port_of("gateway2", 50051)
            def ready():
                with socket.create_connection(("127.0.0.1", second), timeout=1):
                    return True
            self.wait(ready)
            self.run(["docker", "exec", self.names["gateway2"], "sh", "-c",
                      "test ! -e /private.json && test ! -e /sql-password"])
            self.gateway_ports.append(second)
            event.update(actual_gateways=2, data_only=True)
        elif phase == "gateways-one":
            if "gateway2" in self.names:
                self.run(["docker", "stop", "-t", "5", self.names["gateway2"]])
            self.gateway_ports = [self.gateway_port]
        else:
            return
        event["seconds"] = (time.monotonic_ns() - started) / 1e9
        event.setdefault("status", "passed")
        self.transitions.append(event)
        save(self.root / "transitions.json", self.transitions)

    def measure(self):
        for phase, jobs, concurrency, rows in PHASES:
            self.change_topology(phase)
            assignment_before = self.post({"op": "view"})
            cpu_before = resource.getrusage(resource.RUSAGE_SELF)
            self.observer.phase(phase)
            start = time.monotonic_ns()
            inputs = []
            for i in range(jobs):
                key = "main" if phase == "elephant-table" else ("main", "cas")[i % 2]
                inputs.append((self.next_marker, rows, key, phase, self.gateway_ports[i % len(self.gateway_ports)]))
                self.next_marker += rows
            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                records = list(pool.map(self.put, inputs))
            elapsed = (time.monotonic_ns() - start) / 1e9
            self.records.extend(records)
            save(self.root / "benchmark-inputs.json", self.records)
            self.observer.finish(phase)
            cpu_after = resource.getrusage(resource.RUSAGE_SELF)
            value = {"phase": phase, "jobs": jobs, "concurrency": concurrency, "rows_per_job": rows,
                "seconds": elapsed, "offered_rows": jobs * rows, "completed_rows": sum(
                    r["rows"] for r in records if r["completion"] != "unresolved"),
                "terminal": distribution([r["terminal_ms"] for r in records if r["terminal"] == "final"]),
                "admission_identity_observed": distribution([r["admission_identity_observed_ms"] for r in records
                                                           if "admission_identity_observed_ms" in r]),
                "final_receipt": distribution([r["final_receipt_ms"] for r in records if "final_receipt_ms" in r]),
                "reader_visibility_observed": distribution([r["reader_visibility_ms"] for r in records
                                                          if r["reader_visibility_ms"] is not None]),
                "generator_cpu_seconds": cpu_after.ru_utime + cpu_after.ru_stime - cpu_before.ru_utime - cpu_before.ru_stime,
                "generator_peak_rss_kib": cpu_after.ru_maxrss, "host_load_average": os.getloadavg(),
                "assignment_before": assignment_before, "assignment_after": self.post({"op": "view"}),
                "actual_gateways": len(self.gateway_ports),
                "comparable": len(self.gateway_ports) == (2 if phase == "gateways-two" else 1)}
            value["rows_per_second"] = value["completed_rows"] / elapsed
            value["logical_arrow_bytes_per_second"] = value["rows_per_second"] * 8
            self.phases.append(value)
            save(self.root / "phases.json", self.phases)
            print(json.dumps({"arm": self.arm, "repetition": self.repetition, "phase": phase,
                              "rows_per_second": value["rows_per_second"], "terminal": value["terminal"]}), flush=True)
        self.reconcile()

    def reconcile(self):
        # Separate delta-rs process, ordinary Delta reader, exact marker multiset.
        result = self.run([sys.executable, str(Path(__file__)), "--scan", str(self.root)])
        reconciliation = json.loads(result.stdout)
        sql = json.loads(self.sql_query("SET NOCOUNT ON; SELECT table_id,operation_id,version,digest,"
                                       "completed_at FROM delta_authority.receipts FOR JSON PATH;"))
        operations = {r["operation_id"]: r for r in sql}
        completed = [r for r in self.records if r["completion"] != "unresolved"]
        for record in completed:
            final = record["receipts"][-1]
            actual = operations[final["operation"]]
            assert actual["table_id"] == record["table"] and actual["version"] == final["version"]
            assert actual["digest"] == final["digest"]
        schema_receipts = [r for r in sql if r["operation_id"].endswith("-schema")]
        assert len(sql) == len(completed) + len(schema_receipts), "unattributed SQL completion; retain unknown outcome"
        assert len(schema_receipts) == 2
        save(self.root / "sql-final-receipts.json", sql)
        reconciliation.update(final_input_receipts=len(completed), schema_receipts=len(schema_receipts),
                              payload_retries=0, nonfinal_inputs=len(self.records) - len(completed),
                              unresolved_operations=sum(r["completion"] == "unresolved" and bool(r["receipts"])
                                                        for r in self.records))
        save(self.root / "independent-reconciliation.json", reconciliation)
        work = self.root / "delta-private/work"
        usage = {"preparation_attempts": len(list(work.iterdir())),
                 "preparation_limit": 128, "inputs": len(self.records),
                 "sql_state_utf16_bytes": int(self.sql_query(
                     "SET NOCOUNT ON; SELECT DATALENGTH(payload) FROM delta_authority.state;")),
                 "delta_files": len(list((self.root / "delta-private/tables").rglob("*.parquet"))),
                 "delta_parquet_bytes": sum(p.stat().st_size for p in (self.root / "delta-private/tables").rglob("*.parquet")),
                 "delta_log_commits": len(list((self.root / "delta-private/tables").rglob("*.json")))}
        assert len(completed) * 2 <= usage["preparation_attempts"] <= len(self.records) * 2 < 128
        save(self.root / "substrate-costs.json", usage)

    def finish_benchmark(self):
        if self.observer is not None and hasattr(self.observer, "thread"):
            self.observer.stop()
        for role, name in self.names.items():
            result = self.run(["docker", "logs", name])
            (self.root / "logs" / (role + ".log")).write_text(result.stdout + result.stderr)
        self.executor.shutdown(wait=True)
        self.finish()


def independent_scan(root):
    from deltalake import DeltaTable
    records = json.loads((root / "benchmark-inputs.json").read_text())
    summary = {}
    for key in sorted({r["table"] for r in records}):
        submitted = Counter(i for r in records if r["table"] == key for i in range(r["first"], r["first"] + r["rows"]))
        expected = Counter(i for r in records if r["table"] == key and r.get("completion", "stream-final") != "unresolved"
                           for i in range(r["first"], r["first"] + r["rows"]))
        delta = DeltaTable(str(root / "delta-private/tables" / key))
        rows = delta.to_pyarrow_dataset().to_table(columns=["id"], use_threads=False,
            batch_readahead=0, fragment_readahead=1)["id"].to_pylist()
        actual = Counter(rows)
        missing, extra = expected - actual, actual - expected
        summary[key] = {"version": delta.version(), "expected_rows": sum(expected.values()), "actual_rows": len(rows),
                        "submitted_rows": sum(submitted.values()), "missing_submitted_rows": sum((submitted - actual).values()),
                        "missing": sum(missing.values()), "extra_or_duplicate": sum(extra.values()),
                        "marker_sha256": hashlib.sha256(json.dumps(sorted(rows)).encode()).hexdigest()}
        assert not missing and not extra, summary[key]
    print(json.dumps(summary))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort")
    parser.add_argument("--scan", type=Path)
    parser.add_argument("--subnet-start", type=int, default=70)
    parser.add_argument("--resume", action="store_true", help="continue only after retained runs have reconciled")
    args = parser.parse_args()
    if args.scan:
        independent_scan(args.scan)
        return
    assert args.cohort and args.cohort.replace("-", "").isalnum()
    assert sum(p[1] for p in PHASES) * 2 < 128
    directory = REPO / "target/delta-benchmark" / args.cohort
    if args.resume:
        assert directory.is_dir()
        attempts = json.loads((directory / "attempts.json").read_text())
        assert all(a["status"] == "completed-accounted-phases" for a in attempts)
        frozen = directory / "bin"
        resume_sources = directory / ("resume-" + str(time.time_ns()))
        resume_sources.mkdir()
        shutil.copy2(Path(__file__), resume_sources / "delta_benchmark.py")
        execute_remaining(args, directory, frozen, attempts)
        return
    directory.mkdir(parents=True, exist_ok=False)
    revision = {"commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        "dirty": bool(subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=normal"], cwd=REPO, text=True).strip())}
    frozen = directory / "bin"
    frozen.mkdir()
    for binary in BINARIES:
        shutil.copy2(REPO / "target/debug" / binary, frozen / binary)
    selected = REPO / "target/delta-benchmark/selected-cohort.json"
    history = REPO / "projects/loadtest/adr0001"
    prefix = ""
    if selected.exists():
        prior = json.loads(selected.read_text())["cohort"]
        assert prior.replace("-", "").isalnum()
        history = selected.parent / prior
        prefix = "historical-"
        preceding = REPO / "projects/loadtest/adr0001/report.html"
        shutil.copy2(preceding, directory / "preceding-shared-report.html")
        save(directory / "preceding-cohort.json", {
            "cohort": prior, "report_sha256": digest(preceding),
            "closeout": str((history / "closeout.json").relative_to(REPO))})
    for name in ("report.html", "report.py"):
        shutil.copy2(history / (prefix + name), directory / ("historical-" + name))
    source_paths = [REPO / "Cargo.lock", REPO / "Cargo.toml"]
    for base in ("crates/arrowcol-authority", "crates/arrowcol-routing", "crates/arrow-deltalake",
                 "crates/arrowcol-sql-server", "crates/arrowcol-protocol", "crates/arrowcol-receipts",
                 "projects/arrowcol-router-rust", "projects/loadtest/adr0001"):
        source_paths += [p for p in (REPO / base).rglob("*") if p.is_file()
                         and p.suffix in (".rs", ".toml", ".sql", ".py", ".md", ".json")
                         and "__pycache__" not in p.parts]
    save(directory / "source-binary-sha256.json", {
        **{str(p.relative_to(REPO)): digest(p) for p in source_paths},
        **{str((frozen / n).relative_to(REPO)): digest(frozen / n) for n in BINARIES}})
    revision["source_binary_manifest_sha256"] = digest(directory / "source-binary-sha256.json")
    save(directory / "revision.json", revision)
    for source in source_paths:
        destination = directory / "measured-source" / source.relative_to(REPO)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    save(directory / "methodology.json", {"phases": PHASES, "repetitions": 3,
         "arm_order": [["pingora", "adr"], ["adr", "pingora"], ["pingora", "adr"]],
         "rows_logical_bytes": "int64 markers, 8 bytes/row; not wire bytes",
         "admission_metric": "client observes identity emitted after successful admission, not internal duration",
         "decision_acceptance_ms": None, "materialization_ms": None,
         "sql_operations_latency": None, "cache_hits_misses": None,
         "reader": "one standard Delta scan immediately after terminal per input; no periodic polling",
         "tails": "P95 requires >=100 observations; P99 >=1000; smaller P95 explicitly exploratory",
         "no_payload_replay": True, "finite_preparations_including_noop": 112,
         "input_lifetime": "half-close immediately after the single bounded batch in both arms; wait for final then terminal",
         "identity_resolution": "after nonfinal RPC, poll exact operation at 200ms for <=45s; no payload replay"})
    execute_remaining(args, directory, frozen, [])


def execute_remaining(args, directory, frozen, attempts):
    for repetition, order in enumerate((("pingora", "adr"), ("adr", "pingora"), ("pingora", "adr")), 1):
        for arm in order:
            if any(a["repetition"] == repetition and a["arm"] == arm for a in attempts):
                continue
            subnet = str(ipaddress.ip_network(f"10.233.{args.subnet_start + len(attempts)}.0/24"))
            project = f"publication-gate-bench-{args.cohort}-r{repetition}-{arm}"
            runtime = Benchmark(project, subnet, arm, repetition, frozen)
            attempt = {"repetition": repetition, "arm": arm, "project": project,
                       "evidence": str(runtime.root.relative_to(REPO)), "status": "running"}
            attempts.append(attempt)
            save(directory / "attempts.json", attempts)
            try:
                runtime.setup_benchmark()
                runtime.measure()
                attempt["status"] = "completed-accounted-phases"
            except Exception as error:
                attempt.update(status="blocked", error=str(error)[:2500])
                raise
            finally:
                try:
                    runtime.finish_benchmark()
                finally:
                    save(directory / "attempts.json", attempts)
    print("COHORT COMPLETE " + str(directory), flush=True)


if __name__ == "__main__":
    main()
