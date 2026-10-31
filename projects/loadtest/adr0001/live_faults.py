"""Private bounded pause/rollback supervisor, independent of the HTTP process."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import sqlite3
import subprocess
import sys
import threading
import time

from live_contract import Denial
from live_deployment import (MANIFEST, RUNTIME_IMAGE, SQL_IMAGE, deployment_root, describe,
                             private_root, read_private)
from observation import Engine


MAX_DURATION_MS = 3000
FINISHED = frozenset({"repaired", "rejected"})
ALLOWED = {
    "pause": frozenset({"collector1", "collector2", "collector3", "collector4", "gateway", "gateway2", "gateway3"}),
    "publisher-interrupt": frozenset({"publisher"}),
    "authority-loss": frozenset({"sql"}),
}


class FaultResource:
    """A worker can control one saved container, never execute native binaries."""

    def __init__(self, project, subnet, arm, record, engine=None):
        self.root = deployment_root(project)
        private_root(self.root)
        manifest = read_private(self.root / MANIFEST)
        if (manifest.get("version") != 1 or manifest.get("project") != project
                or manifest.get("subnet") != subnet or manifest.get("arm") != arm
                or manifest.get("sql_image") != SQL_IMAGE or manifest.get("runtime_image") != RUNTIME_IMAGE
                or record["intent"]["deployment"] != manifest.get("deployment")):
            raise Denial("wrong-target")
        deployment = manifest["deployment"]
        if (set(deployment) != {"id", "incarnation"} or deployment["id"] != project
                or not re.fullmatch(r"[a-f0-9]{32}", deployment["incarnation"])):
            raise Denial("wrong-target")
        action, native = record["intent"]["action"], record["native"]
        role = action["resource"]["id"].removeprefix("resource-")
        if (set(action) != {"operation", "resource", "fault", "duration_ms"}
                or action["operation"] != "fault" or role not in ALLOWED.get(action["fault"], ())
                or type(action["duration_ms"]) is not int or not 1 <= action["duration_ms"] <= MAX_DURATION_MS):
            raise Denial("unsupported")
        name = project + "-" + role
        if (native["name"] != name or role != "sql" and manifest["names"].get(role) != name
                or manifest["containers"].get(name) != {k: native[k] for k in ("id", "started_at", "image")}
                or not re.fullmatch(r"[a-f0-9]{64}", native["id"])
                or not re.fullmatch(r"sha256:[a-f0-9]{64}", native["image"])
                or role == "sql" and native["image"] != SQL_IMAGE
                or action["resource"] != {"id": "resource-" + role,
                                         "incarnation": hashlib.sha256(
                                             (native["id"] + native["started_at"]).encode()).hexdigest()}):
            raise Denial("wrong-target")
        self.engine = engine or Engine()
        self.project, self.subnet, self.arm, self.deployment = project, subnet, arm, deployment
        self.container_ids = {name: native["id"]}
        if describe(self.engine, name, native["id"], project) != manifest["containers"][name]:
            raise Denial("wrong-target")

    def owned_container(self, name):
        if name not in self.container_ids:
            raise Denial("wrong-target")
        return self.container_ids[name]


def boot_identity():
    return hashlib.sha256(Path("/proc/sys/kernel/random/boot_id").read_bytes()).hexdigest()


class FaultJournal:
    def __init__(self, root):
        private_root(root)
        path = root / "bridge-faults.sqlite"
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)
        self.lock = threading.Lock()
        self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=2)
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("CREATE TABLE IF NOT EXISTS faults (id TEXT PRIMARY KEY,digest TEXT NOT NULL,record TEXT NOT NULL)")
        directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def close(self):
        self.connection.close()

    def reserve(self, operation_id, intent, resource):
        encoded = json.dumps(intent, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute("SELECT digest,record FROM faults WHERE id=?", (operation_id,)).fetchone()
                if row is not None:
                    if row[0] != digest:
                        raise Denial("conflict")
                    self.connection.execute("COMMIT")
                    return json.loads(row[1]), False
                rows = self.connection.execute("SELECT record FROM faults LIMIT 256").fetchall()
                if len(rows) >= 256 or any(json.loads(row[0])["phase"] not in FINISHED for row in rows):
                    raise Denial("limit")
                record = {
                    "operation_id": operation_id, "intent": intent, "native": resource, "phase": "requested",
                    "boot": boot_identity(), "start_by_ns": time.monotonic_ns() + 5_000_000_000,
                    "deadline_ns": None, "applied": False, "repair": False, "error_code": None,
                    "repair_boot": None, "repaired_ns": None,
                }
                self.connection.execute("INSERT INTO faults VALUES (?,?,?)", (operation_id, digest, json.dumps(record)))
                self.connection.execute("COMMIT")
                return record, True
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def get(self, operation_id):
        with self.lock:
            row = self.connection.execute("SELECT record FROM faults WHERE id=?", (operation_id,)).fetchone()
        if row is None:
            raise Denial("unknown-operation")
        return json.loads(row[0])

    def update(self, operation_id, **changes):
        if not set(changes) <= {"phase", "deadline_ns", "applied", "repair", "error_code", "repair_boot", "repaired_ns"}:
            raise ValueError("invalid private fault journal update")
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute("SELECT record FROM faults WHERE id=?", (operation_id,)).fetchone()
                if row is None:
                    raise Denial("unknown-operation")
                record = json.loads(row[0])
                record.update(changes)
                self.connection.execute("UPDATE faults SET record=? WHERE id=?", (json.dumps(record), operation_id))
                self.connection.execute("COMMIT")
                return record
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def unresolved(self):
        with self.lock:
            rows = self.connection.execute("SELECT record FROM faults ORDER BY rowid LIMIT 256").fetchall()
        return [record for row in rows if (record := json.loads(row[0]))["phase"] not in FINISHED]


def exact_state(runtime, record):
    native = record["native"]
    if describe(runtime.engine, native["name"], native["id"], runtime.project) != {
        key: native[key] for key in ("id", "started_at", "image")
    }:
        raise Denial("wrong-target")
    return runtime.engine.get(f"/containers/{native['id']}/json")["State"]


def docker_effect(operation, identity):
    if operation not in ("pause", "unpause") or not re.fullmatch(r"[a-f0-9]{64}", identity):
        raise ValueError("invalid bounded supervisor effect")
    result = subprocess.run(["docker", operation, identity], capture_output=True, timeout=2)
    if result.returncode != 0:
        raise RuntimeError("bounded exact-container supervisor effect failed")


def supervise(runtime, journal, operation_id, repair_only=False, effect=docker_effect, authorize=None):
    record = journal.get(operation_id)
    if record["intent"]["deployment"] != runtime.deployment:
        raise Denial("wrong-target")
    if record["phase"] in FINISHED:
        return record
    descriptor = os.open(runtime.root / "bridge-fault-owner.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record = journal.get(operation_id)
        try:
            if record["phase"] == "requested":
                if (repair_only or record["repair"] or record["boot"] != boot_identity()
                        or time.monotonic_ns() >= record["start_by_ns"]):
                    return journal.update(operation_id, phase="rejected", error_code="timeout")
                state = exact_state(runtime, record)
                if not state["Running"] or state.get("Paused"):
                    return journal.update(operation_id, phase="rejected", error_code="conflict")
                if authorize is not None:
                    authorize()
                state = exact_state(runtime, record)
                if not state["Running"] or state.get("Paused"):
                    return journal.update(operation_id, phase="rejected", error_code="conflict")
                record = journal.get(operation_id)
                if record["repair"] or time.monotonic_ns() >= record["start_by_ns"]:
                    return journal.update(operation_id, phase="rejected", error_code="timeout")
                record = journal.update(
                    operation_id, phase="pause-sent",
                    deadline_ns=time.monotonic_ns() + record["intent"]["action"]["duration_ms"] * 1_000_000)
                effect("pause", record["native"]["id"])
                if not exact_state(runtime, record).get("Paused"):
                    raise RuntimeError("pause effect was not observed")
                record = journal.update(operation_id, phase="paused", applied=True)
            # A resumed worker only compensates; it never repeats an uncertain pause.
            if not repair_only and record["phase"] == "paused" and record["boot"] == boot_identity():
                while time.monotonic_ns() < record["deadline_ns"]:
                    if journal.get(operation_id)["repair"]:
                        break
                    time.sleep(0.05)
        except (OSError, ValueError, RuntimeError, KeyError, TypeError, Denial, subprocess.TimeoutExpired) as error:
            print("local fault supervisor unavailable: " + type(error).__name__, flush=True)
            phase = journal.get(operation_id)["phase"]
            journal.update(operation_id, phase="rejected" if phase == "requested" else phase,
                           error_code=error.code if isinstance(error, Denial) else "unknown")
        finally:
            record = journal.get(operation_id)
            if record["phase"] not in FINISHED and record["phase"] != "requested":
                journal.update(operation_id, phase="rollback-pending")
                try:
                    state = exact_state(runtime, record)
                    if state.get("Paused"):
                        effect("unpause", record["native"]["id"])
                    state = exact_state(runtime, record)
                    if not state["Running"] or state.get("Paused"):
                        raise RuntimeError("bounded rollback was not observed")
                    journal.update(operation_id, phase="repaired",
                                   repair_boot=boot_identity(), repaired_ns=time.monotonic_ns())
                except (OSError, ValueError, RuntimeError, KeyError, TypeError, Denial, subprocess.TimeoutExpired) as error:
                    print("local fault repair required: " + type(error).__name__, flush=True)
                    journal.update(operation_id, phase="repair-required", error_code="unknown")
        return journal.get(operation_id)
    finally:
        os.close(descriptor)


class FaultSupervisor:
    """Only trusted, already-authorized callers can reserve an effect."""

    def __init__(self, runtime):
        self.runtime = runtime
        self.journal = FaultJournal(runtime.root)
        self.children = {}

    def launch(self, operation_id, action, authorize):
        if (not isinstance(operation_id, str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", operation_id)
                or not isinstance(action, dict)
                or set(action) != {"operation", "resource", "fault", "duration_ms"}
                or action["operation"] != "fault" or action["fault"] not in ALLOWED
                or type(action["duration_ms"]) is not int or not 1 <= action["duration_ms"] <= MAX_DURATION_MS
                or not isinstance(action["resource"], dict) or set(action["resource"]) != {"id", "incarnation"}
                or not isinstance(action["resource"]["id"], str) or not callable(authorize)):
            raise Denial("unsupported")
        intent = {"deployment": self.runtime.deployment, "action": action}
        try:
            known = self.journal.get(operation_id)
        except Denial as error:
            if error.code != "unknown-operation":
                raise
        else:
            if known["intent"] != intent:
                raise Denial("conflict")
            return known
        role = action["resource"]["id"].removeprefix("resource-")
        if role not in ALLOWED[action["fault"]]:
            raise Denial("wrong-target")
        name = self.runtime.sql_name if role == "sql" else self.runtime.names.get(role)
        if name is None:
            raise Denial("wrong-target")
        identity = self.runtime.owned_container(name)
        native = describe(self.runtime.engine, name, identity, self.runtime.project)
        if action["resource"] != {
            "id": "resource-" + role,
            "incarnation": hashlib.sha256((identity + native["started_at"]).encode()).hexdigest(),
        }:
            raise Denial("wrong-target")
        record, fresh = self.journal.reserve(
            operation_id, intent, {**native, "name": name})
        if not fresh:
            return record
        log_path = self.runtime.root / ("fault-worker-" + hashlib.sha256(operation_id.encode()).hexdigest() + ".log")
        ready_read, ready_write = os.pipe()
        try:
            try:
                descriptor = os.open(log_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
                with os.fdopen(descriptor, "w") as log:
                    child = subprocess.Popen(
                        [sys.executable, "-B", str(Path(__file__)), "--project", self.runtime.project,
                         "--subnet", str(self.runtime.subnet), "--arm", self.runtime.arm,
                         "--operation-id", operation_id, "--ready-fd", str(ready_write)],
                        stdin=subprocess.PIPE, stdout=log, stderr=subprocess.STDOUT,
                        pass_fds=(ready_write,), start_new_session=True,
                    )
            except (OSError, ValueError):
                self.journal.update(operation_id, phase="rejected", error_code="failed")
                raise
            finally:
                os.close(ready_write)
            self.children[operation_id] = child
            try:
                if not select.select([ready_read], [], [], 6)[0] or os.read(ready_read, 1) != b"R":
                    raise Denial("timeout")
                authorize()
                child.stdin.write(b"G")
                child.stdin.flush()
            finally:
                child.stdin.close()
        finally:
            os.close(ready_read)
        return self.journal.get(operation_id)

    def repair(self, operation_id):
        self.journal.update(operation_id, repair=True)
        if operation_id in self.children:
            self.children[operation_id].wait(timeout=12)
        deadline = time.monotonic() + 12
        while True:
            try:
                return supervise(self.runtime, self.journal, operation_id, repair_only=True)
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("independent fault owner did not release within repair bound") from None
                time.sleep(0.05)

    def close(self):
        for record in self.journal.unresolved():
            self.journal.update(record["operation_id"], repair=True)
        for child in self.children.values():
            child.wait(timeout=12)
        for record in self.journal.unresolved():
            result = self.repair(record["operation_id"])
            if result["phase"] not in FINISHED:
                raise RuntimeError("exact-resource fault repair remains unresolved")
        self.journal.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--subnet", required=True)
    parser.add_argument("--arm", choices=("adr", "pingora"), required=True)
    parser.add_argument("--operation-id", required=True)
    parser.add_argument("--repair-only", action="store_true")
    parser.add_argument("--ready-fd", type=int)
    args = parser.parse_args()
    if not args.repair_only and (args.ready_fd is None or not 3 <= args.ready_fd < 4096):
        raise ValueError("new faults require a live parent authorization handshake")
    def authorize():
        os.write(args.ready_fd, b"R")
        os.close(args.ready_fd)
        descriptor = sys.stdin.fileno()
        if not select.select([descriptor], [], [], 5)[0] or os.read(descriptor, 1) != b"G":
            raise Denial("disarmed")
    journal = FaultJournal(deployment_root(args.project))
    try:
        runtime = FaultResource(args.project, args.subnet, args.arm, journal.get(args.operation_id))
        result = supervise(runtime, journal, args.operation_id, args.repair_only,
                           authorize=None if args.repair_only else authorize)
        if result["phase"] not in FINISHED:
            raise RuntimeError("fault remains unresolved; exact-resource repair required")
    finally:
        journal.close()


if __name__ == "__main__":
    main()
