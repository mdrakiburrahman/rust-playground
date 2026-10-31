"""Server-owned finite workload presets; receipt completion is a separate observation."""
import copy
import hashlib
import json
import re
import subprocess
import time
import uuid

from exchange import unique_object
from live_contract import Denial, SNAPSHOT_AGE_NS, validate_packet
from live_deployment import describe


PRESETS = (
    {"profile": "small", "table_id": "main", "schema": "baseline", "streams": 1,
     "rows_per_request": 3, "maximum_requests": 8, "maximum_duration_ms": 10000},
    {"profile": "elephant", "table_id": "main", "schema": "baseline", "streams": 1,
     "rows_per_request": 65536, "maximum_requests": 2, "maximum_duration_ms": 15000},
)


def preset_for(action):
    if not isinstance(action, dict) or set(action) != {"operation", "profile", "requests", "duration_ms", "streams"}:
        raise Denial("unsupported")
    preset = next((item for item in PRESETS if item["profile"] == action["profile"]), None)
    if (action["operation"] != "workload" or preset is None
            or type(action["requests"]) is not int or not 1 <= action["requests"] <= preset["maximum_requests"]
            or type(action["duration_ms"]) is not int
            or not 1 <= action["duration_ms"] <= preset["maximum_duration_ms"]
            or type(action["streams"]) is not int or not 1 <= action["streams"] <= preset["streams"]):
        raise Denial("unsupported")
    return copy.deepcopy(preset)


class WorkloadJournal:
    def __init__(self, ledger):
        self.ledger = ledger
        with ledger.lock:
            ledger.connection.execute("CREATE TABLE IF NOT EXISTS workloads (id TEXT PRIMARY KEY,record TEXT NOT NULL)")

    def create(self, operation_id, deployment):
        intent = self.ledger.intent(operation_id, deployment)
        action = intent["action"]
        preset = preset_for(action)
        with self.ledger.lock:
            connection = self.ledger.connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute("SELECT record FROM workloads WHERE id=?", (operation_id,)).fetchone()
                if row is not None:
                    connection.execute("COMMIT")
                    return json.loads(row[0]), False
                count = connection.execute("SELECT COUNT(*) FROM workloads").fetchone()[0]
                if count >= 256:
                    raise Denial("limit")
                # Private monotone allocation avoids collisions between workload jobs.
                base = 1_000_000_000 + count * 1_048_576
                alias = hashlib.sha256((deployment["incarnation"] + "\0" + operation_id).encode()).hexdigest()[:32]
                job = {
                    "deployment": deployment, "phase": "prepared", "preset": preset, "streams": action["streams"],
                    "attempts": [{
                        "request_id": "workload-" + alias + "-" + str(index),
                        "attempt_id": "workload-" + alias + "-" + str(index) + "-attempt-0",
                        "first": base + index * preset["rows_per_request"], "rows": preset["rows_per_request"],
                        "sent": False, "unit": None, "rpc_status": "requested", "error_code": None,
                        "final": None, "close_error": None,
                    } for index in range(action["requests"])],
                }
                connection.execute("INSERT INTO workloads VALUES (?,?)", (operation_id, json.dumps(job)))
                connection.execute("COMMIT")
                return job, True
            except BaseException:
                connection.execute("ROLLBACK")
                raise

    def get(self, operation_id, deployment):
        self.ledger.intent(operation_id, deployment)
        with self.ledger.lock:
            row = self.ledger.connection.execute("SELECT record FROM workloads WHERE id=?", (operation_id,)).fetchone()
        if row is None:
            raise Denial("unknown-operation")
        job = json.loads(row[0])
        if job["deployment"] != deployment:
            raise Denial("wrong-target")
        return job

    def update(self, operation_id, deployment, index=None, **changes):
        self.ledger.intent(operation_id, deployment)
        allowed = {"sent", "unit", "rpc_status", "error_code", "final", "close_error"} if index is not None else {"phase"}
        if not set(changes) <= allowed:
            raise ValueError("invalid private workload update")
        with self.ledger.lock:
            connection = self.ledger.connection
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute("SELECT record FROM workloads WHERE id=?", (operation_id,)).fetchone()
                if row is None:
                    raise Denial("unknown-operation")
                job = json.loads(row[0])
                if job["deployment"] != deployment:
                    raise Denial("wrong-target")
                target = job if index is None else job["attempts"][index]
                if target.get("sent") and changes.get("sent") is False:
                    raise ValueError("workload attempts cannot be replayed")
                target.update(changes)
                raw = json.dumps(job, allow_nan=False)
                if len(raw.encode()) > 32768:
                    raise ValueError("private workload record exceeds bound")
                connection.execute("UPDATE workloads SET record=? WHERE id=?", (raw, operation_id))
                connection.execute("COMMIT")
                return job
            except BaseException:
                connection.execute("ROLLBACK")
                raise


class NativeWorkload:
    def __init__(self, runtime):
        self.runtime = runtime

    def prepare(self):
        import pyarrow as pa
        from publication_gate import REPO
        runtime = self.runtime
        name = runtime.names["gateway"]
        identity = runtime.owned_container(name)
        record = runtime.records[name]
        if describe(runtime.engine, name, identity, runtime.project) != record:
            raise Denial("wrong-target")
        state = runtime.engine.get(f"/containers/{identity}/json")
        if not state["State"]["Running"] or state["State"].get("Paused"):
            raise Denial("disconnected")
        bindings = state["NetworkSettings"]["Ports"]["50051/tcp"]
        if len(bindings) != 1 or bindings[0]["HostIp"] != "127.0.0.1":
            raise Denial("wrong-target")
        self.port = int(bindings[0]["HostPort"])
        if not 1024 <= self.port <= 65535:
            raise Denial("wrong-target")
        table = next(item for item in runtime.config["tables"] if item["key"] == "main")
        if table["path"] != ["telemetry", "runtime", "main"]:
            raise Denial("wrong-target")
        self.fingerprint = table["fingerprints"][0]
        path = runtime.root / "schema-0.arrow"
        if path.is_symlink() or path.stat().st_size > 262144:
            raise ValueError("invalid dedicated workload schema")
        self.schema = pa.ipc.open_stream(str(path)).schema
        if not self.schema.equals(pa.schema([pa.field("id", pa.int64(), nullable=False)])):
            raise Denial("wrong-target")
        fingerprint = subprocess.run([str(REPO / "target/debug/adr-fingerprint"), str(path)],
                                     capture_output=True, timeout=3)
        if fingerprint.returncode != 0 or fingerprint.stdout.decode().strip() != self.fingerprint:
            raise Denial("wrong-target")

    def send(self, attempt, remaining, remember):
        import pyarrow as pa
        import pyarrow.flight as flight
        from delta_runtime import native
        started = time.monotonic()
        headers = {
            "content-type": "application/grpc", "x-arrowcol-protocol-version": "1",
            "x-arrowcol-implementation-pool": "direct", "x-arrowcol-catalog": "telemetry",
            "x-arrowcol-schema": "runtime", "x-arrowcol-table": "main",
            "x-arrowcol-schema-fingerprint": self.fingerprint,
        }
        headers = native.signed_headers(self.runtime.config["edge_secret"].encode(), "POST", native.PUT_PATH, headers)
        headers["x-adr-trace"] = attempt["attempt_id"]
        batch = pa.RecordBatch.from_arrays(
            [pa.array(range(attempt["first"], attempt["first"] + attempt["rows"]), type=pa.int64())],
            schema=self.schema)
        remaining -= time.monotonic() - started
        if remaining < 0.001:
            raise TimeoutError("workload dispatch deadline elapsed")
        client = flight.FlightClient(f"grpc://127.0.0.1:{self.port}")
        writer = None
        try:
            writer, reader = client.do_put(
                flight.FlightDescriptor.for_path("telemetry", "runtime", "main", "v1"), self.schema,
                flight.FlightCallOptions(timeout=remaining, headers=[(key.encode(), value.encode())
                                                                    for key, value in headers.items()]))
            writer.write_batch(batch)
            writer.done_writing()
            unit = None
            for kind in ("DELTA_OPERATION", "DELTA_COMMITTED"):
                raw = reader.read()
                if raw is None or raw.size > 4096:
                    raise ValueError("invalid bounded native workload receipt")
                receipt = json.loads(raw.to_pybytes(), object_pairs_hook=unique_object)
                if receipt["kind"] != kind:
                    raise ValueError("unexpected native workload receipt")
                if kind == "DELTA_OPERATION":
                    unit = receipt["operation"]
                    if set(receipt) != {"kind", "operation", "final"} or receipt["final"] is not False:
                        raise ValueError("invalid native unit receipt")
                    if not isinstance(unit, str) or str(uuid.UUID(unit)) != unit:
                        raise ValueError("invalid native unit identity")
                    remember(unit=unit)
                else:
                    if (set(receipt) != {"kind", "operation", "version", "digest"}
                            or receipt["operation"] != unit or type(receipt["version"]) is not int
                            or not 0 <= receipt["version"] < 128
                            or not isinstance(receipt["digest"], str)
                            or not re.fullmatch(r"[a-f0-9]{64}", receipt["digest"])):
                        raise ValueError("invalid native final receipt")
                    remember(final={"version": str(receipt["version"]), "digest": receipt["digest"]})
            if reader.read() is not None:
                raise ValueError("native workload receipt count exceeded")
            writer.close()
            writer = None
            remember(rpc_status="final")
        finally:
            try:
                if writer is not None:
                    try:
                        writer.close()
                    except (pa.ArrowException, flight.FlightError, OSError, RuntimeError) as error:
                        print("live workload close unavailable: " + type(error).__name__, flush=True)
                        remember(close_error=type(error).__name__)
            finally:
                client.close()


class WorkloadRunner:
    def __init__(self, ledger, native, snapshot):
        self.journal = WorkloadJournal(ledger)
        self.native = native
        self.snapshot = snapshot

    def run(self, operation_id, deployment, authorize, admitted, stopping):
        import pyarrow as pa
        import pyarrow.flight as flight
        job, fresh = self.journal.create(operation_id, deployment)
        if not fresh:
            return self.outcome(operation_id, deployment)
        self.receipt_health(deployment)
        self.native.prepare()
        self.receipt_health(deployment)
        admitted()
        self.journal.update(operation_id, deployment, phase="running")
        action = self.journal.ledger.intent(operation_id, deployment)["action"]
        deadline = time.monotonic() + action["duration_ms"] / 1000
        for index, attempt in enumerate(job["attempts"]):
            remaining = deadline - time.monotonic()
            if stopping() or remaining <= 0:
                break
            # Current qualified presets admit exactly one concurrent stream.
            if job["streams"] != 1:
                raise Denial("unqualified")
            authorize()
            self.receipt_health(deployment)
            self.journal.update(operation_id, deployment, index, sent=True, rpc_status="pending")
            def remember(**changes):
                self.journal.update(operation_id, deployment, index, **changes)
            try:
                self.native.send(attempt, remaining, remember)
            except (pa.ArrowException, flight.FlightError, OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
                print("live workload response unavailable: " + type(error).__name__, flush=True)
                timed_out = isinstance(error, (TimeoutError, flight.FlightTimedOutError))
                remember(rpc_status="timeout" if timed_out else "unknown",
                         error_code="timeout" if timed_out else "unknown")
                break
        job = self.journal.get(operation_id, deployment)
        for index, attempt in enumerate(job["attempts"]):
            if not attempt["sent"]:
                self.journal.update(operation_id, deployment, index, rpc_status="cancelled", error_code="disarmed")
        self.journal.update(operation_id, deployment, phase="finished")
        return self.outcome(operation_id, deployment)

    def receipt_health(self, deployment):
        snapshot = validate_packet(self.snapshot())
        freshness = snapshot["freshness"]
        if (snapshot["deployment"] != deployment or freshness["status"] != "fresh"
                or freshness["age_ns"] is None or int(freshness["age_ns"]) > SNAPSHOT_AGE_NS):
            raise Denial("stale")
        if snapshot["receipt_observation_known"] is not True:
            raise Denial("disconnected")
        return snapshot

    def outcome(self, operation_id, deployment):
        job = self.journal.get(operation_id, deployment)
        if any(attempt["sent"] and attempt["unit"] is None for attempt in job["attempts"]):
            return "unknown", None
        if any(not attempt["sent"] for attempt in job["attempts"]):
            return "cancelled", None
        snapshot = self.receipt_health(deployment)
        freshness = snapshot["freshness"]
        if (snapshot["deployment"] != deployment or freshness["status"] != "fresh"
                or freshness["age_ns"] is None or int(freshness["age_ns"]) > SNAPSHOT_AGE_NS):
            raise Denial("stale")
        for attempt in job["attempts"]:
            receipt = next((item for item in snapshot["receipts"]
                            if item["table_id"] == job["preset"]["table_id"]
                            and item["operation_id"] == attempt["unit"]), None)
            if receipt is None or receipt["completion"] != "completed" or receipt["digest"] is None:
                return "unknown", None
            if attempt["final"] is not None and any(
                receipt[field] != attempt["final"][field] for field in ("version", "digest")
            ):
                return "unknown", None
        return "completed", snapshot["revision"]
