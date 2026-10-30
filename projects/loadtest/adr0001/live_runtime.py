"""Explicit single-deployment Linux/Docker observation host; no browser effects."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import threading
import time
import urllib.error
import urllib.request

from jsonschema.exceptions import ValidationError

from delta_runtime import Runtime
from live_contract import Denial, RESPONSE_BYTES
from live_ledger import OperationLedger
from live_server import BoundedHTTPServer, Bridge
from observation import Engine
from publication_gate import RUNTIME_IMAGE, SQL_IMAGE


def require_local_images():
    for key, image in (("ARROWCOL_SQL_IMAGE", SQL_IMAGE), ("ARROWCOL_RUNTIME_IMAGE", RUNTIME_IMAGE)):
        if os.environ.get(key) != image or not re.fullmatch(r"(?:[a-z0-9./:_-]+@)?sha256:[a-f0-9]{64}", image):
            raise ValueError("dedicated bridge requires explicit immutable local image pins")
        result = subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", image],
                                capture_output=True, text=True, timeout=10)
        if (result.returncode != 0 or not re.fullmatch(r"sha256:[a-f0-9]{64}", result.stdout.strip())
                or image.startswith("sha256:") and result.stdout.strip() != image):
            raise ValueError("qualified local image unavailable; bridge never pulls images")


def writer_alias(writer):
    return {"collector": writer["collector"], "process": writer["process"], "generation": str(writer["generation"])}


class RuntimeObserver:
    def __init__(self, runtime, bridge, engine=None):
        self.runtime = runtime
        self.bridge = bridge
        self.engine = engine or Engine()
        self.stop = threading.Event()
        self.clock = "observer-" + secrets.token_hex(16)
        self.origin = time.monotonic_ns()
        self.previous = {}
        self.dropped = 0
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.thread = None

    def authority(self):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.runtime.control_port}/v1/authority",
            data=b'{"op":"view"}',
            headers={"Authorization": "Bearer " + self.runtime.config["operator_token"],
                     "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            data = response.read(RESPONSE_BYTES + 1)
            if len(data) > RESPONSE_BYTES:
                raise ValueError("authority observation exceeds bound")
            return json.loads(data)

    def component(self, item):
        role, name = item
        identity = self.runtime.owned_container(name)
        state = self.engine.get(f"/containers/{identity}/json")
        labels = state["Config"].get("Labels") or {}
        if state["Id"] != identity or not (
            labels.get("delta-runtime") == self.runtime.project or labels.get("publication-gate") == self.runtime.project
        ):
            raise ValueError("deployment resource identity mismatch")
        resource = {"id": "resource-" + role,
                    "incarnation": hashlib.sha256((identity + state["State"]["StartedAt"]).encode()).hexdigest()}
        stats = None
        if state["State"]["Running"]:
            stats = self.engine.get(f"/containers/{identity}/stats?stream=false&one-shot=true")
        return role, resource, state["State"]["Running"], stats, time.monotonic_ns()

    def sample(self):
        started = time.monotonic_ns()
        state = self.authority()
        members = {member["writer"]["collector"]: member for member in state["members"]}
        policies = state["policies"]
        assignments = {item["record"]["key"]: item["record"] for item in state["assignments"]}
        resources = dict(self.runtime.names)
        resources["sql"] = self.runtime.sql_name
        results = list(self.pool.map(self.component, sorted(resources.items())))
        replicas, metrics = [], []
        dropped = self.dropped
        missing = {"receipts", "queue-depth", "queue-age", "throughput", "io-read", "io-write"}
        if dropped:
            missing.add("observation-gap")
        for role, resource, running, stats, observed in results:
            member = members.get(role)
            writer = writer_alias(member["writer"]) if member else None
            policy = policies.get(role)
            if policy and policy.get("incarnation") != (member["writer"] if member else None):
                raise ValueError("policy observation has foreign incarnation")
            lifecycle = "unknown"
            if not running:
                lifecycle = "retired" if policy and policy["retiring"] and policy["approved"] else "failed"
            elif member and member["eligible"]:
                lifecycle = "active"
            elif role == self.runtime.control_role:
                lifecycle = "active"
            if running and member:
                for assignment in assignments.values():
                    if assignment["writer"] == member["writer"]:
                        lifecycle = assignment["lifecycle"].lower()
                if policy and policy["retiring"]:
                    lifecycle = "draining"
            replicas.append({
                "resource": resource, "role": ("collector" if role.startswith("collector") else
                    "storage" if role == "sql" else "publisher" if role == "publisher" else
                    "gateway" if role.startswith("gateway") else "authority"),
                "status": lifecycle, "writer": writer,
                "policy_revision": str(policy["revision"]) if policy else ("0" if member else None),
                "applied_policy_revision": str(policy["applied_revision"]) if policy else ("0" if member else None),
                "excluded": policy["excluded"] if policy else (False if member else None),
                "block_streams": policy["block_streams"] if policy else (False if member else None),
                "retiring": policy["retiring"] if policy else (False if member else None),
            })
            previous = self.previous.get(role)
            current_cpu = (stats or {}).get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage")
            if current_cpu is not None and (
                not isinstance(current_cpu, int) or isinstance(current_cpu, bool) or current_cpu < 0
            ):
                raise ValueError("invalid CPU counter")
            memory = (stats or {}).get("memory_stats", {}).get("usage")
            cpu, duration, offset = None, 0, observed - self.origin
            valid_baseline = True
            if previous and previous["resource"] == resource and current_cpu is not None:
                duration = observed - previous["observed"]
                if duration <= 0 or current_cpu < previous["cpu"]:
                    missing.add("cpu-counter-gap")
                    dropped = min(dropped + 1, 2147483647)
                    duration = 0
                    valid_baseline = False
                else:
                    cpu = current_cpu - previous["cpu"]
                    offset = previous["observed"] - self.origin
            for name, value, unit, span, start in (
                ("cpu", cpu, "ns", duration, offset),
                ("memory", memory, "bytes", 0, observed - self.origin),
            ):
                if value is not None and (not isinstance(value, int) or isinstance(value, bool) or value < 0):
                    raise ValueError("invalid resource counter")
                if value is None:
                    missing.add(name)
                metrics.append({
                    "id": role + "-" + name, "resource": resource,
                    "process_id": "cgroup-" + resource["incarnation"][:24],
                    "clock_id": self.clock, "offset_ns": str(start), "duration_ns": str(span),
                    "metric": name, "value": str(value) if value is not None else None, "unit": unit,
                    "evidence": "measured" if value is not None else "unavailable",
                    "method": "process-counter" if value is not None else "not-observed",
                })
            if current_cpu is not None and valid_baseline:
                self.previous[role] = {"resource": resource, "cpu": current_cpu, "observed": observed}
            else:
                self.previous.pop(role, None)
        tables = []
        for configured in self.runtime.config["tables"]:
            key = configured["key"]
            record = assignments.get(key)
            tables.append({
                "id": key, "epoch": str(record["epoch"]) if record else "0",
                "lifecycle": record["lifecycle"].lower() if record else "unknown",
                "writer": writer_alias(record["writer"]) if record else None,
                "recovery_obligations": ["obligation-" + collector for collector, policy in policies.items()
                                         if key in policy.get("recovery_obligations", {})],
            })
        self.bridge.publish(replicas, tables, [], metrics, list(missing), observed_ns=started, dropped=dropped)
        self.dropped = 0

    def start(self):
        def observe():
            while not self.stop.is_set():
                started = time.monotonic()
                try:
                    self.sample()
                except (OSError, ValueError, KeyError, TypeError, RuntimeError,
                        http.client.HTTPException, Denial, ValidationError, urllib.error.URLError) as error:
                    # Preserve the last observation as stale; never fabricate successful telemetry.
                    self.dropped = min(self.dropped + 1, 2147483647)
                    print("live bridge observation unavailable: " + type(error).__name__, flush=True)
                self.stop.wait(max(0, 1 - (time.monotonic() - started)))
        self.thread = threading.Thread(target=observe, name="local-poc-observer", daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=10)
            if self.thread.is_alive():
                raise RuntimeError("observer failed to stop within bound")
        self.pool.shutdown(wait=True, cancel_futures=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="new explicit dedicated publication-gate-* deployment")
    parser.add_argument("--subnet", required=True)
    parser.add_argument("--arm", choices=("adr", "pingora"), required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--frontend-root", type=Path, required=True)
    parser.add_argument("--bootstrap-file", type=Path, required=True)
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        raise ValueError("loopback port bound")
    if args.bootstrap_file.exists() or args.bootstrap_file.is_symlink():
        raise ValueError("bootstrap file must be a new private file")
    frontend = args.frontend_root.resolve(strict=True)
    if args.bootstrap_file.resolve().is_relative_to(frontend):
        raise ValueError("bootstrap material must remain outside static files")
    require_local_images()
    runtime = Runtime(args.project, args.subnet, args.arm, "sql")
    ledger, observer, server = None, None, None
    def stop(*_):
        if server is None:
            raise InterruptedError("live bridge startup interrupted")
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        runtime.root.chmod(0o700)
        runtime.setup_runtime()
        if runtime.root.resolve().is_relative_to(frontend):
            raise ValueError("runtime secrets must remain outside static files")
        ledger = OperationLedger(runtime.root / "bridge-operations.sqlite")
        deployment = {"id": args.project, "incarnation": secrets.token_hex(16)}
        bridge = Bridge(f"http://127.0.0.1:{args.port}", deployment, args.frontend_root, ledger)
        server = BoundedHTTPServer(("127.0.0.1", args.port), bridge)
        observer = RuntimeObserver(runtime, bridge)
        observer.start()
        descriptor = os.open(args.bootstrap_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        try:
            os.write(descriptor, (bridge.origin + "/#arrowcol-grant=" + bridge.grant).encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

        print("authenticated read-only local bridge ready; private bootstrap file written", flush=True)
        server.serve_forever(poll_interval=0.25)
    finally:
        if server is not None:
            server.server_close()
        try:
            if observer is not None:
                observer.close()
        finally:
            try:
                if ledger is not None:
                    ledger.close()
            finally:
                runtime.finish_runtime()


if __name__ == "__main__":
    main()
