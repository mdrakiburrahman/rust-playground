"""Finite, task-scoped benchmark scenarios; not a test replacement or a retry loop."""

import argparse
import importlib.metadata
import json
import os
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

from stack import context, control, move, scale_in


class Experiment:
    def __init__(self, root, label, repetitions=3):
        self.root = Path(root).resolve()
        self.identity, self.compose = context(self.root)
        self.output = self.root / "evidence" / (label + "-" + uuid.uuid4().hex[:8])
        self.output.mkdir(parents=True)
        self.run_token = uuid.uuid4().hex[:16]
        self.tables = json.loads((self.root / "prepared/results/prepared-tables.json").read_text())
        self.endpoints = ["router"]
        self.stress = []
        self.workers = []
        self.repetitions = repetitions
        self.resource_sampler = None
        self.sampled_ids = []

    def stop_resource_sampler(self):
        if self.resource_sampler is not None:
            self.resource_sampler.terminate()
            try:
                self.resource_sampler.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.resource_sampler.kill()
                self.resource_sampler.wait(timeout=5)
            self.resource_sampler = None

    def capture(self, name):
        view = control(self.root, "view")["value"]
        (self.output / (name + "-authority.json")).write_text(json.dumps(view, indent=2))
        ids = sorted(subprocess.check_output(self.compose + ["ps", "-q"], text=True).split())
        running = subprocess.check_output(self.compose + ["ps", "--services"], text=True).split()
        for service in ("router", "router2"):
            if service not in running:
                continue
            address = subprocess.check_output(self.compose + ["port", service, "50050"], text=True).strip()
            if not address.startswith("127.0.0.1:"):
                raise ValueError("router metrics require loopback-only developer ingress")
            with urllib.request.urlopen("http://" + address + "/metrics", timeout=5) as response:
                metrics = response.read(1024 * 1024 + 1)
            if len(metrics) > 1024 * 1024:
                raise RuntimeError("router metrics exceeded the bounded evidence snapshot")
            (self.output / f"{name}-{service}-metrics.txt").write_text(metrics.decode("utf-8"))
        if ids:
            if ids != self.sampled_ids:
                self.stop_resource_sampler()
                with (self.output / (name + "-resource-samples.jsonl")).open("w") as samples:
                    self.resource_sampler = subprocess.Popen(
                        ["docker", "--context", "default", "stats", "--format", "{{json .}}", *ids],
                        stdout=samples, stderr=subprocess.STDOUT,
                    )
                self.sampled_ids = ids
            stats = subprocess.check_output(
                ["docker", "--context", "default", "stats", "--no-stream", "--format", "{{json .}}", *ids],
                text=True,
            )
            if self.resource_sampler.poll() is not None:
                raise RuntimeError("task-scoped Docker resource sampler exited unexpectedly")
            (self.output / (name + "-docker-stats.jsonl")).write_text(stats)
        return view

    def wait_ready(self, count):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                view = control(self.root, "view")["value"]
                ready = [c for c in view["collectors"] if c["state"] == "ready"
                         and (c.get("pressure") or {}).get("accepting_deliveries")
                         and time.time_ns() // 1_000_000 - c["pressure"]["observed_at_unix_ms"] <= 3000]
                if len(ready) >= count:
                    return view
            except (RuntimeError, subprocess.CalledProcessError):
                pass
            time.sleep(1)
        raise TimeoutError("fresh ready collector count did not converge")

    def settings(self, label, tables, **overrides):
        endpoints = []
        for service in self.endpoints:
            address = subprocess.check_output(self.compose + ["port", service, "50050"], text=True).strip()
            if not address.startswith("127.0.0.1:"):
                raise ValueError("benchmark requires loopback-only developer ingress")
            endpoints.append("grpc+tcp://" + address)
        receipt_address = subprocess.check_output(self.compose + ["port", "router", "50050"], text=True).strip()
        settings = {
            "run_id": self.run_token + "-" + label, "workers": 3, "rows": 32, "streams": 12,
            "batches_per_stream": 2, "timeout": 30, "stream_seconds": 15, "pacing_ms": 0,
            "max_batch_bytes": 1024 * 1024, "publication_timeout": 60, "duration_seconds": 30,
            "endpoints": endpoints, "receipt_endpoint": "http://" + receipt_address,
            "edge_secret_file": str(self.root / "secrets/edge_hmac"),
            "prepared_tables": str(self.root / "prepared/results/prepared-tables.json"),
            "sdk_base": str(self.root / "prepared"), "delta_root": str(self.root / "direct"),
            "tables": tables, "results": str(self.output / label),
        }
        settings.update(overrides)
        path = self.output / (label + "-settings.json")
        path.write_text(json.dumps(settings, indent=2))
        return path

    def workload(self, label, tables, background=False, locust=False, **overrides):
        settings = self.settings(label, tables, **overrides)
        command = [sys.executable, str(Path(__file__).with_name("workload.py")), str(settings)]
        environment = os.environ.copy()
        if locust:
            environment["ARROWCOL_WORKLOAD_SETTINGS"] = str(settings)
            # Flight runs in native ThreadPool workers; Locust monkey-patching is
            # unnecessary and would also patch the worker's blocking HTTP client.
            environment["LOCUST_SKIP_MONKEY_PATCH"] = "1"
            command = [
                sys.executable, "-m", "locust", "-f", str(Path(__file__).with_name("locustfile.py")),
                "--headless", "-u", "3", "-r", "3", "--run-time", "40s", "--stop-timeout", "35",
                "--csv", str(self.output / (label + "-locust")),
            ]
        log = (self.output / (label + ".log")).open("w")
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=environment)
        log.close()
        self.workers.append(process)
        if background:
            return process
        if process.wait(timeout=180) != 0:
            raise RuntimeError(f"{label} did not reconcile; preserve {self.output / label}")
        return json.loads((self.output / label / "report.json").read_text())

    def pressure(self, service, seconds=18):
        command = self.compose + [
            "exec", "-T", service, "timeout", str(seconds), "sh", "-c",
            'while :; do timeout 0.15 sh -c "while :; do :; done"; sleep 0.05; done',
        ]
        log = (self.output / (service + "-bounded-pressure.log")).open("w")
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        log.close()
        self.stress.append((process, seconds))
        return process

    def cleanup_pressure(self):
        for process, seconds in self.stress:
            code = process.wait(timeout=seconds + 10)
            if code not in (0, 124, 137, 143):
                raise RuntimeError("bounded in-container CPU pressure did not execute")
        self.stress.clear()

    def cleanup_workloads(self):
        for process in self.workers:
            if process.poll() is None:
                try:
                    process.wait(timeout=180)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
        (self.output / "worker-exit-codes.json").write_text(
            json.dumps([process.returncode for process in self.workers])
        )

    def cold_warm_pressure(self):
        identities = [table["identity"] for table in self.tables]
        self.capture("initial")
        self.workload("cold", identities[:3])
        before = self.capture("cold")
        for repetition in range(self.repetitions):
            label = f"warm-locust-{repetition + 1}"
            self.workload(label, identities[:3], locust=True)
            after = self.capture(label)
            if owners(before) != owners(after):
                raise RuntimeError("ordinary warm traffic changed sticky ownership")
        self.pressure("direct-1")
        time.sleep(3)
        self.capture("unequal-pressure")
        self.workload("new-table-pressure", identities[3:9], streams=18)
        self.capture("pressure-placements")
        self.cleanup_pressure()

    def migration(self):
        view = self.wait_ready(3)
        lease = next(lease for lease in view["leases"] if lease["state"] == "Active")
        collector = next(c for c in view["collectors"] if c["registration"]["instance_id"] == lease["holder_instance_id"])
        service = "direct-" + collector["registration"]["instance_key"].rsplit("-", 1)[1]
        table = ".".join(lease["key"]["table"][name] for name in ("catalog", "schema", "table"))
        worker = self.workload("admitted-before-move", [table], background=True, workers=1,
                               streams=1, batches_per_stream=32, pacing_ms=250, stream_seconds=20)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            current = control(self.root, "view")["value"]
            source = next(c for c in current["collectors"] if c["registration"]["instance_id"] == lease["holder_instance_id"])
            if (source.get("pressure") or {}).get("active_streams", {}).get("current", 0) > 0:
                break
            if worker.poll() is not None:
                raise RuntimeError("source stream ended before migration admission evidence")
            time.sleep(0.25)
        else:
            raise TimeoutError("no admitted source stream observed")
        self.pressure(service, seconds=18)
        time.sleep(2)
        self.capture("migration-source-active")
        result = move(self.root, lease["key"])
        (self.output / "controlled-migration.json").write_text(json.dumps(result, indent=2))
        if worker.wait(timeout=120) != 0:
            raise RuntimeError("admitted source stream/publication did not reconcile")
        if result["lease"]["fencing_epoch"] <= lease["fencing_epoch"]:
            raise RuntimeError("migration did not durably advance routing epoch")
        self.cleanup_pressure()
        time.sleep(2)
        self.workload("after-move", [table], streams=3)
        self.capture("migration-complete")

    def scaling(self):
        identities = [table["identity"] for table in self.tables]
        before = self.capture("collectors-three")
        subprocess.run(self.compose + ["up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "direct-4"], check=True)
        view = self.wait_ready(4)
        registration_observed_at = time.time_ns() // 1_000_000
        fourth = next(c for c in view["collectors"] if c["registration"]["instance_key"].endswith("-4"))
        self.capture("fourth-registered")
        # The new container has no physical-capacity claim. Fresh new-table
        # assignments and controlled ownership moves demonstrate plumbing only.
        self.workload("four-collectors-new-tables", identities[9:], streams=24)
        after = self.capture("collectors-four")
        if not any(lease["holder_instance_id"] == fourth["registration"]["instance_id"] for lease in after["leases"]):
            raise RuntimeError("fourth collector did not receive a fresh table assignment")
        fourth_tables = {
            ".".join(lease["key"]["table"][field] for field in ("catalog", "schema", "table"))
            for lease in after["leases"] if lease["holder_instance_id"] == fourth["registration"]["instance_id"]
        }
        streams = [json.loads(line) for line in
                   (self.output / "four-collectors-new-tables/streams.jsonl").read_text().splitlines()]
        first_receipt_at = min(receipt["observed_at_unix_ms"] for stream in streams
                               if stream["table"] in fourth_tables for receipt in stream["receipts"])
        (self.output / "collector-registration-to-work.json").write_text(json.dumps({
            "instance_id": fourth["registration"]["instance_id"],
            "registration_observed_at_unix_ms": registration_observed_at,
            "first_receipt_observed_at_unix_ms": first_receipt_at,
            "registration_observed_to_first_receipt_ms": first_receipt_at - registration_observed_at,
            "process_start_to_first_receipt_ms": first_receipt_at - fourth["registration"]["started_at_unix_ms"],
            "observation_not_exact_server_registration_time": True,
        }, indent=2))
        if any(owners(after).get(key) != owner for key, owner in owners(before).items()):
            raise RuntimeError("scale-out moved an established table without a controlled move")
        existing = min((lease for lease in before["leases"] if lease["state"] == "Active"),
                       key=lambda lease: lease["fencing_epoch"])
        for service in ("direct-1", "direct-2", "direct-3"):
            self.pressure(service)
        time.sleep(3)
        self.capture("new-capacity-pressure")
        inbound = move(self.root, existing["key"])
        (self.output / "collector-scale-out-migration.json").write_text(json.dumps(inbound, indent=2))
        if inbound["lease"]["holder_instance_id"] != fourth["registration"]["instance_id"]:
            raise RuntimeError("controller did not choose the fresh fourth collector under recorded old-pool pressure")
        self.cleanup_pressure()
        moved_table = ".".join(existing["key"]["table"][field] for field in ("catalog", "schema", "table"))
        self.workload("after-new-capacity-move", [moved_table], streams=3)
        self.capture("new-capacity-migration-complete")
        evidence = scale_in(self.root)
        if not evidence:
            raise RuntimeError("scale-in did not exercise a controlled table migration")
        (self.output / "collector-scale-in.json").write_text(json.dumps(evidence, indent=2))
        self.capture("collectors-back-to-three")
        self.workload("after-collector-removal", identities[9:], streams=8)
        self.router_scaling()

    def router_scaling(self):
        identities = [table["identity"] for table in self.tables]
        subprocess.run(self.compose + ["up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "router2"], check=True)
        time.sleep(3)
        self.endpoints = ["router", "router2"]
        before = self.capture("two-data-routers")
        connections = self.workload("two-router-connections", identities[:3], streams=24)
        if any(row["terminal_ok"] == 0 for row in connections["endpoint_streams"].values()):
            raise RuntimeError("new connections did not succeed through both data routers")
        if any(path.name != "config.yaml" for path in (self.root / "router2-state").iterdir()):
            raise RuntimeError("data-only router created unexpected authority state")
        # Stop while a finite stream on router2 remains admitted. Pingora gets its
        # normal graceful drain; removal must not require any table migration.
        self.endpoints = ["router2"]
        worker = self.workload("router2-drain", identities[:1], background=True, workers=1,
                               streams=1, batches_per_stream=24, pacing_ms=200)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            view = control(self.root, "view")["value"]
            if any((collector.get("pressure") or {}).get("active_streams", {}).get("current", 0) > 0
                   for collector in view["collectors"]):
                self.capture("router2-active-before-drain")
                break
            if worker.poll() is not None:
                raise RuntimeError("router2 stream ended before admission was observed")
            time.sleep(0.25)
        else:
            raise TimeoutError("no admitted router2 stream observed")
        subprocess.run(self.compose + ["stop", "--timeout", "95", "router2"], check=True)
        if worker.wait(timeout=120) != 0:
            raise RuntimeError("added data router did not finish its admitted stream")
        stopped = json.loads(subprocess.check_output(self.compose + ["ps", "-a", "--format", "json", "router2"], text=True))
        if stopped.get("ExitCode") != 0:
            raise RuntimeError("added data router was not gracefully stopped")
        after = self.capture("one-data-router")
        if owners(before) != owners(after):
            raise RuntimeError("data-router scaling changed table ownership")
        self.endpoints = ["router"]
        self.workload("after-router-removal", identities[:3])

    def restart(self):
        before = self.capture("before-controller-restart")
        subprocess.run(self.compose + ["restart", "--timeout", "5", "router"], check=True)
        self.wait_ready(3)
        after = self.capture("after-controller-restart")
        if owners(before) != owners(after):
            raise RuntimeError("controller restart changed established lease lineage")
        self.workload("restart-existing-owner", [self.tables[0]["identity"]], streams=3)

    def finish(self):
        self.stop_resource_sampler()
        logs = subprocess.check_output(self.compose + ["logs", "--no-color", "router", "router2"], text=True)
        (self.output / "controller-router.log").write_text(logs)
        collectors = subprocess.check_output(
            self.compose + ["logs", "--no-color", "direct-1", "direct-2", "direct-3", "direct-4"], text=True,
        )
        (self.output / "collectors.log").write_text(collectors)
        (self.output / "python-package-versions.json").write_text(json.dumps({
            name: importlib.metadata.version(name) for name in ("pyarrow", "locust", "deltalake")
        }, indent=2))
        (self.output / "versions.txt").write_text(subprocess.check_output(
            ["docker", "--context", "default", "version", "--format", "{{.Server.Version}}"], text=True))
        (self.output / "scope.json").write_text(json.dumps({
            "project": self.identity["project"], "policy": self.identity["policy"],
            "same_host_containers": True, "physical_capacity_added_claim": False,
            "exactly_once_claim": False, "automatic_doput_replay": False,
            "schema_fingerprints": {table["identity"]: table["fingerprint"] for table in self.tables},
            "warm_repetitions": self.repetitions,
        }, indent=2))


def owners(view):
    return {json.dumps(lease["key"], sort_keys=True):
            (lease["holder_instance_id"], lease["fencing_epoch"], lease["lease_id"]) for lease in view["leases"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--repetitions", type=int, choices=(1, 2, 3), default=3)
    parser.add_argument("--scenario", choices=("cold-warm-pressure", "migration", "scaling", "data-routers", "restart", "all"), default="all")
    arguments = parser.parse_args()
    experiment = Experiment(arguments.root, arguments.scenario, arguments.repetitions)
    try:
        experiment.wait_ready(3)
        if arguments.scenario in ("cold-warm-pressure", "all"):
            experiment.cold_warm_pressure()
        if arguments.scenario in ("migration", "all"):
            experiment.migration()
        if arguments.scenario in ("scaling", "all"):
            experiment.scaling()
        if arguments.scenario == "data-routers":
            experiment.router_scaling()
        if arguments.scenario in ("restart", "all"):
            experiment.restart()
    finally:
        try:
            experiment.cleanup_workloads()
            experiment.cleanup_pressure()
        finally:
            experiment.finish()
            print(experiment.output)
