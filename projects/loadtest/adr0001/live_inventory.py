"""Private fleet creation bookkeeping; not readiness, retirement approval or an executor."""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import stat
import threading
from urllib.parse import quote

from live_contract import Denial
from live_deployment import MANIFEST, private_root, read_private


def serialized(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def record_identity(record):
    if (not isinstance(record, dict) or set(record) != {"id", "started_at", "image"}
            or not isinstance(record["id"], str) or not isinstance(record["image"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", record["id"])
            or not isinstance(record["started_at"], str) or not 1 <= len(record["started_at"]) <= 64
            or not re.fullmatch(r"sha256:[a-f0-9]{64}", record["image"])):
        raise ValueError("invalid fleet resource identity")
    return record


def managed_slot(project, name):
    suffix = name.removeprefix(project + "-")
    if name == project + "-gateway":
        return "gateway", 1
    match = re.fullmatch(r"(collector|gateway)([1-4])", suffix)
    if match and name.startswith(project + "-") and suffix != "gateway1":
        return match[1], int(match[2])
    return None


class FleetInventory:
    """One private writer, bounded durable intents and exact nonce-bound reconciliation.

    A bound resource is not necessarily ACTIVE. No method creates/stops a
    container, grants an operation, clears a tombstone or authorizes retirement.
    """

    def __init__(self, root, deployment, limit=64):
        if type(limit) is not int or not 1 <= limit <= 64:
            raise ValueError("fleet intent bound")
        self.root = Path(root)
        private_root(self.root)
        manifest = read_private(self.root / MANIFEST)
        if (not isinstance(manifest, dict) or not isinstance(deployment, dict)
                or manifest.get("version") != 1 or manifest.get("deployment") != deployment
                or set(deployment) != {"id", "incarnation"}
                or manifest.get("project") != deployment["id"]):
            raise Denial("wrong-target")
        self.deployment = serialized(deployment)
        self.project = deployment["id"]
        self.incarnation = deployment["incarnation"]
        if (not re.fullmatch(r"publication-gate-[A-Za-z0-9-]+", self.project)
                or not re.fullmatch(r"[a-f0-9]{32}", self.incarnation)):
            raise ValueError("invalid inventory deployment")
        baseline = manifest["containers"]
        if not isinstance(baseline, dict) or not 1 <= len(baseline) <= 12:
            raise ValueError("fleet baseline bound")
        slots = []
        for name, record in baseline.items():
            record_identity(record)
            if name not in {self.project + "-" + role for role in
                            ("controller", "publisher", "sql", "collector", "gateway",
                             "gateway2", "gateway3", "collector1", "collector2", "collector3", "collector4")}:
                raise ValueError("invalid fleet birth resource")
            slot = managed_slot(self.project, name)
            if slot:
                slots.append((*slot, name, serialized(record)))
        if len({record["id"] for record in baseline.values()}) != len(baseline):
            raise ValueError("duplicate fleet birth identity")
        self.images = {role: {baseline[name]["image"] for r, _, name, _ in slots if r == role}
                       for role in ("collector", "gateway")}
        if any(len(images) != 1 for images in self.images.values()):
            raise ValueError("fleet runtime image unavailable or inconsistent")
        self.limit = limit
        self.lock = threading.Lock()
        self.connection = None
        self.descriptor = os.open(self.root / "bridge-fleet-owner.lock",
                                  os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            owner = os.fstat(self.descriptor)
            if not stat.S_ISREG(owner.st_mode) or owner.st_uid != os.getuid() or owner.st_nlink != 1:
                raise ValueError("invalid fleet owner lease")
            os.fchmod(self.descriptor, 0o600)
            fcntl.flock(self.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            path = self.root / "bridge-fleet.sqlite"
            descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
            try:
                info = os.fstat(descriptor)
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                    raise ValueError("invalid fleet database")
                os.fchmod(descriptor, 0o600)
            finally:
                os.close(descriptor)
            self.connection = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
            self.connection.execute("PRAGMA journal_mode=DELETE")
            self.connection.execute("PRAGMA synchronous=FULL")
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS fleet_state "
                "(id INTEGER PRIMARY KEY CHECK(id=1), deployment TEXT NOT NULL, "
                "manifest TEXT NOT NULL, baseline TEXT NOT NULL, revision INTEGER NOT NULL)")
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS fleet_slots "
                "(role TEXT NOT NULL, slot INTEGER NOT NULL, name TEXT NOT NULL UNIQUE, "
                "record TEXT, operation_id TEXT UNIQUE, PRIMARY KEY(role,slot))")
            self.connection.execute(
                "CREATE TABLE IF NOT EXISTS fleet_creations "
                "(operation_id TEXT PRIMARY KEY, role TEXT NOT NULL, slot INTEGER NOT NULL, "
                "nonce TEXT NOT NULL UNIQUE, name TEXT NOT NULL UNIQUE, image TEXT NOT NULL, record TEXT)")
            manifest_hash = hashlib.sha256((self.root / MANIFEST).read_bytes()).hexdigest()
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                state = self.connection.execute(
                    "SELECT deployment,manifest,baseline FROM fleet_state WHERE id=1").fetchone()
                expected = (self.deployment, manifest_hash, serialized(baseline))
                if state is None:
                    self.connection.execute("INSERT INTO fleet_state VALUES (1,?,?,?,0)", expected)
                    self.connection.executemany(
                        "INSERT INTO fleet_slots VALUES (?,?,?,?,NULL)", slots)
                elif state != expected:
                    raise Denial("wrong-target")
                if self.connection.execute("SELECT COUNT(*) FROM fleet_creations").fetchone()[0] > limit:
                    raise Denial("limit")
                self._validate(slots)
                self.connection.execute("COMMIT")
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise
            directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except BaseException:
            if self.connection is not None:
                self.connection.close()
            os.close(self.descriptor)
            raise

    def close(self):
        with self.lock:
            self.connection.close()
            os.close(self.descriptor)

    def _validate(self, baseline_slots):
        expected = [(*row, None) for row in baseline_slots]
        records = [json.loads(row[3]) for row in baseline_slots]
        creations = self.connection.execute("SELECT * FROM fleet_creations").fetchall()
        for operation, role, slot, nonce, name, image, value in creations:
            if (not isinstance(operation, str)
                    or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", operation)
                    or not isinstance(role, str) or role not in self.images
                    or type(slot) is not int or not 1 <= slot <= 4
                    or not isinstance(nonce, str) or not re.fullmatch(r"[a-f0-9]{64}", nonce)
                    or name != f"{self.project}-{role}{slot}-fleet-{nonce[:16]}"
                    or image not in self.images[role]):
                raise ValueError("invalid persisted fleet creation")
            if value is not None:
                record = record_identity(json.loads(value))
                if record["image"] != image:
                    raise ValueError("persisted fleet image changed")
                records.append(record)
            expected.append((role, slot, name, value, operation))
        actual = self.connection.execute("SELECT * FROM fleet_slots").fetchall()
        revision = self.connection.execute("SELECT revision FROM fleet_state WHERE id=1").fetchone()[0]
        if (sorted(actual) != sorted(expected)
                or len({record["id"] for record in records}) != len(records)
                or type(revision) is not int
                or revision != len(creations) + sum(row[-1] is not None for row in creations)):
            raise ValueError("inconsistent persisted fleet inventory")

    def snapshot(self):
        with self.lock:
            self.connection.execute("BEGIN")
            try:
                baseline, revision = self.connection.execute(
                    "SELECT baseline,revision FROM fleet_state WHERE id=1").fetchone()
                slots = self.connection.execute(
                    "SELECT role,slot,name,record,operation_id FROM fleet_slots ORDER BY role,slot").fetchall()
                creations = self.connection.execute(
                    "SELECT operation_id,role,slot,nonce,name,image,record FROM fleet_creations "
                    "ORDER BY operation_id").fetchall()
                self.connection.execute("COMMIT")
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise
        return {"revision": revision, "baseline": json.loads(baseline),
                "slots": [dict(zip(("role", "slot", "name", "record", "operation_id"),
                                  (r, s, n, json.loads(v) if v else None, op)))
                          for r, s, n, v, op in slots],
                "creations": [dict(zip(("operation_id", "role", "slot", "nonce", "name", "image", "record"),
                                      (op, r, s, nonce, n, image, json.loads(v) if v else None)))
                              for op, r, s, nonce, n, image, v in creations]}

    def _advance(self, revision):
        if type(revision) is not int or not 0 <= revision < 256:
            raise Denial("invalid")
        if self.connection.execute(
                "UPDATE fleet_state SET revision=revision+1 WHERE id=1 AND revision=?", (revision,)
        ).rowcount != 1:
            raise Denial("stale")

    def reserve_creation(self, operation_id, role, slot, revision):
        if (not isinstance(operation_id, str)
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", operation_id)
                or not isinstance(role, str)
                or role not in self.images or type(slot) is not int or not 1 <= slot <= 4):
            raise Denial("invalid")
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                prior = self.connection.execute(
                    "SELECT role,slot FROM fleet_creations WHERE operation_id=?", (operation_id,)).fetchone()
                if prior is not None:
                    if prior != (role, slot):
                        raise Denial("conflict")
                    self.connection.execute("COMMIT")
                    return False
                if self.connection.execute("SELECT COUNT(*) FROM fleet_creations").fetchone()[0] >= self.limit:
                    raise Denial("limit")
                if self.connection.execute(
                        "SELECT 1 FROM fleet_slots WHERE role=? AND slot=?", (role, slot)).fetchone():
                    raise Denial("conflict")
                self._advance(revision)
                nonce = secrets.token_hex(32)
                name = f"{self.project}-{role}{slot}-fleet-{nonce[:16]}"
                image = next(iter(self.images[role]))
                self.connection.execute("INSERT INTO fleet_creations VALUES (?,?,?,?,?,?,NULL)",
                                        (operation_id, role, slot, nonce, name, image))
                self.connection.execute("INSERT INTO fleet_slots VALUES (?,?,?,NULL,?)",
                                        (role, slot, name, operation_id))
                self.connection.execute("COMMIT")
                return True
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def bind_creation(self, operation_id, engine, revision):
        """Read-only native reconciliation; missing/ambiguous results never create again."""
        with self.lock:
            row = self.connection.execute(
                "SELECT role,slot,nonce,name,image,record FROM fleet_creations WHERE operation_id=?",
                (operation_id,)).fetchone()
        if row is None:
            raise Denial("unknown-operation")
        role, slot, nonce, name, image, prior = row
        labels = {"delta-runtime": self.project, "bridge-deployment-incarnation": self.incarnation,
                  "bridge-creation-nonce": nonce}
        filters = quote(serialized({"label": [f"{key}={value}" for key, value in labels.items()]}), safe="")
        candidates = engine.get("/containers/json?all=1&limit=2&filters=" + filters)
        if candidates == []:
            raise RuntimeError("fleet creation outcome unknown; reconcile without replay")
        if not isinstance(candidates, list) or len(candidates) != 1:
            raise Denial("conflict")
        if not isinstance(candidates[0], dict):
            raise Denial("wrong-target")
        identity = candidates[0].get("Id")
        if not isinstance(identity, str) or not re.fullmatch(r"[a-f0-9]{64}", identity):
            raise Denial("wrong-target")
        state = engine.get(f"/containers/{identity}/json")
        actual_labels = state["Config"].get("Labels") or {}
        if (state["Id"] != identity or state["Name"] != "/" + name or state["Image"] != image
                or any(actual_labels.get(key) != value for key, value in labels.items())
                or state["State"].get("Running") is not True or state["State"].get("Paused") is not False):
            raise Denial("wrong-target")
        record = record_identity({"id": identity, "started_at": state["State"]["StartedAt"], "image": image})
        baseline_ids = {item["id"] for item in self.snapshot()["baseline"].values()}
        if identity in baseline_ids:
            raise Denial("wrong-target")
        value = serialized(record)
        if prior is not None and prior != value:
            raise Denial("wrong-target")
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                current = self.connection.execute(
                    "SELECT record FROM fleet_creations WHERE operation_id=?", (operation_id,)).fetchone()[0]
                if current is not None:
                    if current != value:
                        raise Denial("wrong-target")
                else:
                    if self.connection.execute(
                            "SELECT 1 FROM fleet_slots WHERE json_extract(record,'$.id')=?",
                            (identity,)).fetchone():
                        raise Denial("wrong-target")
                    self._advance(revision)
                    self.connection.execute("UPDATE fleet_creations SET record=? WHERE operation_id=?",
                                            (value, operation_id))
                    self.connection.execute("UPDATE fleet_slots SET record=? WHERE role=? AND slot=?",
                                            (value, role, slot))
                self.connection.execute("COMMIT")
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise
        return record
