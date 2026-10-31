"""Explicit single-deployment host; read-only unless matching policy proof is supplied."""
import argparse
import copy
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
from delta_benchmark import BINARIES
from live_contract import Denial, RESPONSE_BYTES
from live_deployment import AttachedRuntime, DeploymentLease, deployment_root, save_deployment
from live_effects import EffectExecutor
from live_executor import PolicyExecutor
from live_faults import FaultSupervisor
from live_ledger import OperationLedger
from live_receipts import ReceiptObserver
from live_server import BoundedHTTPServer, Bridge
from live_workload import NativeWorkload, WorkloadRunner
from observation import Engine
from publication_gate import REPO, RUNTIME_IMAGE, SQL_IMAGE


def require_local_images():
    for key, image in (("ARROWCOL_SQL_IMAGE", SQL_IMAGE), ("ARROWCOL_RUNTIME_IMAGE", RUNTIME_IMAGE)):
        if os.environ.get(key) != image or not re.fullmatch(r"(?:[a-z0-9./:_-]+@)?sha256:[a-f0-9]{64}", image):
            raise ValueError("dedicated bridge requires explicit immutable local image pins")
        result = subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", image],
                                capture_output=True, text=True, timeout=10)
        if (result.returncode != 0 or not re.fullmatch(r"sha256:[a-f0-9]{64}", result.stdout.strip())
                or image.startswith("sha256:") and result.stdout.strip() != image):
            raise ValueError("qualified local image unavailable; bridge never pulls images")


def verify_policy_proof(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError("policy qualification must be a regular local artifact")
    with path.open("rb") as stream:
        raw = stream.read(262145)
    if len(raw) > 262144:
        raise ValueError("policy qualification artifact exceeds bound")
    proof = json.loads(raw)
    if not isinstance(proof, dict) or not all(
        type(proof.get(name)) is int for name in (
            "server_exit_code", "exact_owned_containers_stopped", "native_policy_mutations",
            "ordinary_delta_exact_marker_rows")
    ) or not isinstance(proof.get("source_sha256"), dict) or not isinstance(
        proof.get("shared_binaries"), dict
    ) or not (
        proof.get("server_exit_code") == 0 and proof.get("exact_owned_containers_stopped") == 8
        and proof.get("native_policy_mutations") == 4 and proof.get("ordinary_delta_exact_marker_rows") == 9
        and all(proof.get(name) is True for name in (
            "lost_response_reconciled_without_replay", "ledger_reopen",
            "admission_rejected_new_stream_before_receipt", "exclusion_did_not_block_owned_table_streams",
            "stale_policy_and_wrong_resource_denied"))
        and proof.get("sql_image") == SQL_IMAGE and proof.get("runtime_image") == RUNTIME_IMAGE
    ):
        raise ValueError("policy qualification evidence is incomplete")
    for name in ("live_executor.py", "live_ledger.py", "live_contract.py", "contracts/live-v1.schema.json"):
        expected = proof.get("source_sha256", {}).get(name)
        if expected != hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest():
            raise ValueError("policy qualification source mismatch")
    for name in BINARIES:
        if proof.get("shared_binaries", {}).get(name) != hashlib.sha256(
            (REPO / "target/debug" / name).read_bytes()
        ).hexdigest():
            raise ValueError("policy qualification binary mismatch")
    return hashlib.sha256(raw).hexdigest()


def verify_effects_proof(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError("effects qualification must be a regular local artifact")
    with path.open("rb") as stream:
        raw = stream.read(262145)
    if len(raw) > 262144:
        raise ValueError("effects qualification artifact exceeds bound")
    proof = json.loads(raw)
    http_proof = isinstance(proof, dict) and proof.get("proof_kind") == "guarded-http-effects"
    native_evidence = (
        type(proof.get("ordinary_delta_exact_marker_rows")) is int
        and proof["ordinary_delta_exact_marker_rows"] == 65548
        and type(proof.get("bounded_single_attempts")) is int and proof["bounded_single_attempts"] == 4
        and all(proof.get(name) is True for name in (
            "server_owned_small_and_elephant_profiles", "same_id_no_replay_and_journal_reopen",
            "publisher_inflight_completion", "completion_requires_public_physical_receipt_observation",
            "measured_fault_lifecycle_below_requested_plus_5s", "actual_bridge_sigkill_independent_rollback",
            "actual_worker_sigkill_then_emergency_repair"))
    ) if isinstance(proof, dict) else False
    if http_proof:
        native_evidence = (
            type(proof.get("physical_workload_units")) is int and proof["physical_workload_units"] == 4
            and type(proof.get("combined_ordinary_delta_exact_marker_rows")) is int
            and proof["combined_ordinary_delta_exact_marker_rows"] == 65554
            and type(proof.get("native_policy_mutations")) is int and proof["native_policy_mutations"] == 4
            and proof.get("frontend_mutations") is False and proof.get("arm") == "adr"
            and all(proof.get(name) is True for name in (
                "durable_same_id_reconciled_without_replay", "stable_deployment_new_bridge_incarnation",
                "snapshot_revision_watermark_preserved", "old_session_denied", "wrong_resource_and_csrf_denied",
                "independent_rollback_before_restart", "fault_and_workload_overlap",
                "workload_lost_http_response_no_replay", "actual_worker_sigkill_then_emergency_repair",
                "measured_fault_lifecycle_below_requested_plus_5s", "endpoint_closed",
                "actual_parent_death_during_preflight_no_application"))
            and isinstance(proof.get("fault_cases"), list) and len(proof["fault_cases"]) == 5
            and all(isinstance(item, dict) and item.get("phase") == "repaired" and item.get("applied") is True
                    and item.get("fault") in {"pause", "authority-loss", "publisher-interrupt"}
                    and type(item.get("duration_ms")) is int and 1 <= item["duration_ms"] <= 3000
                    and type(item.get("elapsed_ns")) is int
                    and 0 <= item["elapsed_ns"] < (item["duration_ms"] + 5000) * 1_000_000
                    for item in proof["fault_cases"])
            and {item["fault"] for item in proof["fault_cases"]} == {"pause", "authority-loss", "publisher-interrupt"}
            and isinstance(proof.get("effects"), list) and len(proof["effects"]) == 8
            and all(isinstance(item, dict) and item.get("status") == "completed"
                    and item.get("operation") in {"fault", "workload"} for item in proof["effects"])
            and sum(item["operation"] == "fault" for item in proof["effects"]) == 5
            and proof.get("fault_owner_sigkill_exit") == -9 and proof.get("crashed_process_exit") == -9
        )
    if (not isinstance(proof, dict) or type(proof.get("server_exit_code")) is not int
            or proof["server_exit_code"] != 0
            or type(proof.get("exact_owned_containers_stopped")) is not int
            or proof["exact_owned_containers_stopped"] != 8
            or not native_evidence
            or proof.get("arm") != "adr"
            or proof.get("sql_image") != SQL_IMAGE or proof.get("runtime_image") != RUNTIME_IMAGE):
        raise ValueError("effects qualification evidence is incomplete")
    sources = proof.get("source_sha256")
    binaries = proof.get("shared_binaries")
    if not isinstance(sources, dict) or not isinstance(binaries, dict):
        raise ValueError("effects qualification identities unavailable")
    for name in ("live_effects.py", "live_executor.py", "live_faults.py", "live_workload.py", "live_ledger.py",
                 "live_server.py", "live_runtime.py", "live_deployment.py", "live_contract.py", "live_receipts.py",
                 "contracts/live-v1.schema.json"):
        if sources.get(name) != hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest():
            raise ValueError("effects qualification source mismatch")
    for name in BINARIES:
        if binaries.get(name) != hashlib.sha256((REPO / "target/debug" / name).read_bytes()).hexdigest():
            raise ValueError("effects qualification binary mismatch")
    return hashlib.sha256(raw).hexdigest()


def writer_alias(writer):
    return {"collector": writer["collector"], "process": writer["process"], "generation": str(writer["generation"])}


class RuntimeObserver:
    def __init__(self, runtime, bridge, engine=None, receipt_probe=None):
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
        self.receipt_probe = receipt_probe
        self.last_receipts = []

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

    def component(self, item, with_stats=True):
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
        if state["State"]["Running"] and with_stats:
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
        receipts = []
        if self.receipt_probe is not None:
            try:
                receipts, receipt_missing, receipt_dropped = self.receipt_probe()
                missing.discard("receipts")
                missing.update(receipt_missing)
                dropped = min(dropped + receipt_dropped, 2147483647)
                self.last_receipts = copy.deepcopy(receipts)
            except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.TimeoutExpired) as error:
                print("live bridge receipt observation unavailable: " + type(error).__name__, flush=True)
                missing.add("receipt-observation-error")
                dropped = min(dropped + 1, 2147483647)
                receipts = [{**receipt, "completion": "unknown"} for receipt in self.last_receipts]
        self.bridge.publish(replicas, tables, receipts, metrics, list(missing), observed_ns=started, dropped=dropped)
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
    parser.add_argument("--policy-proof", type=Path,
                        help="explicit matching direct-policy qualification; omitted means read-only")
    parser.add_argument("--effects-proof", type=Path,
                        help="additional exact-source preset/fault proof; also requires --policy-proof")
    parser.add_argument("--resume", action="store_true", help="attach only the saved exact owned deployment")
    parser.add_argument("--retain-runtime-on-stop", action="store_true",
                        help="leave the verified owned runtime running after a normal bridge stop for bounded restart")
    args = parser.parse_args()
    if not 1024 <= args.port <= 65535:
        raise ValueError("loopback port bound")
    if args.bootstrap_file.exists() or args.bootstrap_file.is_symlink():
        raise ValueError("bootstrap file must be a new private file")
    frontend = args.frontend_root.resolve(strict=True)
    if args.bootstrap_file.resolve().is_relative_to(frontend):
        raise ValueError("bootstrap material must remain outside static files")
    require_local_images()
    policy_proof = verify_policy_proof(args.policy_proof) if args.policy_proof is not None else None
    if args.effects_proof is not None:
        if policy_proof is None or args.arm != "adr":
            raise ValueError("additional effects require matching policy qualification and the selected ADR PoC")
        verify_effects_proof(args.effects_proof)
    deployment_root(args.project)
    runtime = lease = None
    ledger, observer, server, executor, faults = None, None, None, None, None
    served = False
    def stop(*_):
        if server is None:
            raise InterruptedError("live bridge startup interrupted")
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        if args.resume:
            lease = DeploymentLease(deployment_root(args.project))
            runtime = AttachedRuntime(args.project, args.subnet, args.arm, policy_proof)
        else:
            mask = os.umask(0)
            os.umask(mask)
            if mask & 0o055:
                raise ValueError("dedicated native bind directories require umask 022 or equivalent")
            runtime = Runtime(args.project, args.subnet, args.arm, "sql")
            runtime.root.chmod(0o700)
            lease = DeploymentLease(runtime.root)
            runtime.setup_runtime()
        if runtime.root.resolve().is_relative_to(frontend):
            raise ValueError("runtime secrets must remain outside static files")
        ledger = OperationLedger(runtime.root / "bridge-operations.sqlite")
        deployment = (runtime.deployment if args.resume else
                      {"id": args.project, "incarnation": secrets.token_hex(16)})
        bridge = Bridge(f"http://127.0.0.1:{args.port}", deployment, args.frontend_root, ledger)
        if not args.resume:
            save_deployment(runtime, deployment, policy_proof)
        attached = None
        if args.effects_proof is not None or (runtime.root / "bridge-faults.sqlite").exists():
            attached = AttachedRuntime(args.project, args.subnet, args.arm, policy_proof)
            faults = FaultSupervisor(attached)
            for record in faults.journal.unresolved():
                if faults.repair(record["operation_id"])["phase"] not in ("repaired", "rejected"):
                    raise RuntimeError("saved exact-resource fault still requires repair")
        observer = RuntimeObserver(runtime, bridge, receipt_probe=ReceiptObserver(runtime).sample)
        if args.policy_proof is not None:
            def resource(collector):
                if collector not in runtime.config["collectors"] or collector not in runtime.names:
                    raise Denial("wrong-target")
                _, identity, running, _, _ = observer.component((collector, runtime.names[collector]), with_stats=False)
                return identity if running else None
            if args.effects_proof is None:
                executor = PolicyExecutor(ledger, runtime.post, resource, bridge.snapshot)
            else:
                workload = WorkloadRunner(ledger, NativeWorkload(attached), bridge.snapshot)
                executor = EffectExecutor(ledger, runtime.post, resource, bridge.snapshot, faults, workload)
            bridge.executor = executor
        server = BoundedHTTPServer(("127.0.0.1", args.port), bridge)
        observer.start()
        descriptor = os.open(args.bootstrap_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        try:
            os.write(descriptor, (bridge.origin + "/#arrowcol-grant=" + bridge.grant).encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

        print("authenticated local bridge ready; private bootstrap file written; policy effects "
              + ("explicitly configured" if executor is not None else "disabled"), flush=True)
        server.serve_forever(poll_interval=0.25)
        served = True
    finally:
        if server is not None:
            server.server_close()
        try:
            if observer is not None:
                observer.close()
        finally:
            try:
                if executor is not None:
                    executor.close()
            finally:
                try:
                    try:
                        if faults is not None:
                            faults.close()
                    finally:
                        if ledger is not None:
                            ledger.close()
                finally:
                    try:
                        if runtime is not None:
                            if served and args.retain_runtime_on_stop:
                                if not args.resume:
                                    runtime.executor.shutdown(wait=True, cancel_futures=True)
                            else:
                                runtime.finish_runtime()
                    finally:
                        if lease is not None:
                            lease.close()


if __name__ == "__main__":
    main()
