"""Bounded durable operation intent; never executes or retries runtime effects."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import threading

from live_contract import Denial, validate_packet


TERMINAL = frozenset({"completed", "failed", "cancelled", "rejected"})
TRANSITIONS = {
    "requested": frozenset({"pending", "failed", "unknown", "cancelled", "timeout", "rejected"}),
    "pending": frozenset({"applied", "completed", "failed", "unknown", "cancelled", "timeout"}),
    "applied": frozenset({"completed", "failed", "unknown", "timeout"}),
    "unknown": frozenset({"pending", "applied", "completed", "failed", "cancelled", "timeout"}),
    "timeout": frozenset({"pending", "applied", "completed", "failed", "unknown"}),
}


class OperationLedger:
    def __init__(self, path, limit=256):
        if not 1 <= limit <= 256:
            raise ValueError("operation ledger limit")
        path = Path(path)
        if path.is_symlink() or path.parent.is_symlink():
            raise ValueError("operation ledger must not be a symlink")
        descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)
        self.lock = threading.Lock()
        self.limit = limit
        self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=DELETE")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, digest TEXT NOT NULL, "
            "intent TEXT NOT NULL, result TEXT NOT NULL)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS bridge_state (id INTEGER PRIMARY KEY CHECK(id=1), "
            "deployment TEXT NOT NULL, revision TEXT NOT NULL)"
        )
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def close(self):
        with self.lock:
            self.connection.close()

    def bind_deployment(self, deployment):
        serialized = json.dumps(deployment, sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute("SELECT deployment,revision FROM bridge_state WHERE id=1").fetchone()
                if row is not None:
                    if row[0] != serialized:
                        raise Denial("wrong-target")
                    revision = int(row[1])
                else:
                    revision = 0
                    for stored, in self.connection.execute("SELECT result FROM operations LIMIT 256").fetchall():
                        result = json.loads(stored)
                        if result["deployment"] != deployment:
                            raise Denial("wrong-target")
                        revision = max(revision, int(result["requested_revision"]),
                                       int(result["applied_revision"] or "0"))
                    self.connection.execute("INSERT INTO bridge_state VALUES (1,?,?)", (serialized, str(revision)))
                self.connection.execute("COMMIT")
                return revision
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def publish_revision(self, deployment, previous, revision):
        if revision != previous + 1 or revision >= 10**20:
            raise Denial("limit")
        serialized = json.dumps(deployment, sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self.lock:
            updated = self.connection.execute(
                "UPDATE bridge_state SET revision=? WHERE id=1 AND deployment=? AND revision=?",
                (str(revision), serialized, str(previous)),
            )
            if updated.rowcount != 1:
                raise Denial("conflict")

    def reserve(self, command):
        """Caller must authenticate/fence new work before reserving; no effects here."""
        validate_packet(command)
        if command["kind"] != "command":
            raise Denial("invalid")
        intent = {name: command[name] for name in ("deployment", "revision", "action")}
        serialized = json.dumps(intent, sort_keys=True, separators=(",", ":"), allow_nan=False)
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        result = {
            "kind": "operation", "schema_version": 1, "operation_id": command["operation_id"],
            "deployment": command["deployment"], "requested_revision": command["revision"],
            "applied_revision": None, "status": "requested", "error_code": None,
        }
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute(
                    "SELECT digest,result FROM operations WHERE id=?", (command["operation_id"],)
                ).fetchone()
                if row is not None:
                    if row[0] != digest:
                        raise Denial("conflict")
                    self.connection.execute("COMMIT")
                    return json.loads(row[1]), False
                count = self.connection.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
                if count >= self.limit:
                    raise Denial("limit")
                self.connection.execute(
                    "INSERT INTO operations (id,digest,intent,result) VALUES (?,?,?,?)",
                    (command["operation_id"], digest, serialized, json.dumps(result, sort_keys=True)),
                )
                self.connection.execute("COMMIT")
                return result, True
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def get(self, operation_id, deployment):
        with self.lock:
            row = self.connection.execute("SELECT result FROM operations WHERE id=?", (operation_id,)).fetchone()
        if row is None:
            raise Denial("unknown-operation")
        result = json.loads(row[0])
        if result["deployment"] != deployment:
            raise Denial("wrong-target")
        return result

    def advance(self, operation_id, deployment, status, applied_revision=None, error_code=None):
        """Only a trusted reconciler can advance; browser requests cannot set outcomes."""
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                row = self.connection.execute("SELECT result FROM operations WHERE id=?", (operation_id,)).fetchone()
                if row is None:
                    raise Denial("unknown-operation")
                result = json.loads(row[0])
                if result["deployment"] != deployment:
                    raise Denial("wrong-target")
                previous = result["status"]
                applied_revision = result["applied_revision"] if applied_revision is None else applied_revision
                if status in ("applied", "completed") and applied_revision is None:
                    raise Denial("invalid")
                if (previous == status and result["applied_revision"] == applied_revision
                        and result["error_code"] == error_code):
                    self.connection.execute("COMMIT")
                    return result
                same_state_observation = previous == status and result["applied_revision"] == applied_revision
                if (previous in TERMINAL
                        or not same_state_observation and status not in TRANSITIONS.get(previous, frozenset())):
                    raise Denial("conflict")
                result.update(status=status, applied_revision=applied_revision, error_code=error_code)
                validate_packet(result)
                self.connection.execute(
                    "UPDATE operations SET result=? WHERE id=?", (json.dumps(result, sort_keys=True), operation_id)
                )
                self.connection.execute("COMMIT")
                return result
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def intent(self, operation_id, deployment):
        """Trusted restart reconciliation only; contains no browser authority."""
        with self.lock:
            row = self.connection.execute("SELECT intent FROM operations WHERE id=?", (operation_id,)).fetchone()
        if row is None:
            raise Denial("unknown-operation")
        intent = json.loads(row[0])
        if intent["deployment"] != deployment:
            raise Denial("wrong-target")
        return intent

    def unresolved(self):
        """Restart inspection is not replay authorization."""
        with self.lock:
            rows = self.connection.execute("SELECT result FROM operations ORDER BY rowid LIMIT 256").fetchall()
        return [result for row in rows if (result := json.loads(row[0]))["status"] not in TERMINAL]
