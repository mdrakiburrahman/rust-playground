"""Private ownership manifest and exclusive restart attachment, never discovery."""
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import subprocess
import urllib.request

from exchange import unique_object
from live_contract import Denial, RESPONSE_BYTES
from observation import Engine
from publication_gate import REPO, RUNTIME_IMAGE, SQL_IMAGE


MANIFEST = "bridge-deployment.json"


def deployment_root(project):
    if not (re.fullmatch(r"publication-gate-[a-zA-Z0-9-]+", project) and len(project + "-sql") <= 63):
        raise ValueError("invalid dedicated deployment identity")
    return REPO / "target/publication-gate" / project


def private_root(root):
    value = root.stat()
    if root.is_symlink() or value.st_uid != os.getuid() or value.st_mode & 0o077:
        raise ValueError("deployment evidence must be owner-only")


def read_private(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        value = os.fstat(stream.fileno())
        if not stat.S_ISREG(value.st_mode) or value.st_uid != os.getuid():
            raise ValueError("invalid private deployment artifact")
        raw = stream.read(131073)
    if len(raw) > 131072:
        raise ValueError("deployment artifact exceeds bound")
    return json.loads(raw, object_pairs_hook=unique_object,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("invalid deployment JSON")))


def write_private_new(path, value):
    temporary = path.with_name("." + path.name + "-" + secrets.token_hex(16))
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink()


class DeploymentLease:
    def __init__(self, root):
        private_root(root)
        self.descriptor = os.open(root / "bridge-owner.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(self.descriptor, 0o600)
            fcntl.flock(self.descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self.descriptor)
            raise

    def close(self):
        os.close(self.descriptor)


def describe(engine, name, identity, project):
    state = engine.get(f"/containers/{identity}/json")
    labels = state["Config"].get("Labels") or {}
    if (state["Id"] != identity or state["Name"] != "/" + name
            or not (labels.get("delta-runtime") == project or labels.get("publication-gate") == project)):
        raise ValueError("deployment ownership mismatch")
    return {"id": identity, "started_at": state["State"]["StartedAt"], "image": state["Image"]}


def qualified_binary(root, name, digest):
    if not re.fullmatch(r"(?:adr-[a-z-]+|arrowcol-router-rust|delta-contract|publication-gate)", name):
        raise ValueError("invalid deployment binary name")
    path = root / "bin" / name
    if (path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != digest
            or hashlib.sha256((REPO / "target/debug" / name).read_bytes()).hexdigest() != digest):
        raise ValueError("deployment binary changed")


def save_deployment(runtime, deployment, policy_proof, engine=None):
    engine = engine or Engine()
    private_root(runtime.root)
    state = {
        "version": 1, "project": runtime.project, "subnet": str(runtime.subnet), "arm": runtime.arm,
        "deployment": deployment, "sql_image": SQL_IMAGE, "runtime_image": RUNTIME_IMAGE,
        "policy_proof": policy_proof, "names": runtime.names, "control_role": runtime.control_role,
        "control_port": int(runtime.control_port),
        "containers": {name: describe(engine, name, identity, runtime.project)
                       for name, identity in runtime.container_ids.items()},
        "binaries": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                     for path in (runtime.root / "bin").iterdir() if path.is_file()},
    }
    if not 1 <= len(state["containers"]) <= 12 or not state["binaries"]:
        raise ValueError("invalid dedicated deployment resource bound")
    for name, digest in state["binaries"].items():
        qualified_binary(runtime.root, name, digest)
    write_private_new(runtime.root / MANIFEST, state)


class AttachedRuntime:
    """Only observation, qualified policy RPCs and exact-incarnation teardown."""

    def __init__(self, project, subnet, arm, policy_proof, engine=None):
        self.root = deployment_root(project)
        private_root(self.root)
        state = read_private(self.root / MANIFEST)
        if (not isinstance(state, dict) or state.get("version") != 1
                or state.get("project") != project or state.get("subnet") != subnet or state.get("arm") != arm
                or state.get("sql_image") != SQL_IMAGE or state.get("runtime_image") != RUNTIME_IMAGE
                or policy_proof is not None and state.get("policy_proof") != policy_proof):
            raise ValueError("deployment attachment identity mismatch")
        self.engine = engine or Engine()
        self.project, self.subnet, self.arm = project, subnet, arm
        self.deployment = state["deployment"]
        if (set(self.deployment) != {"id", "incarnation"} or self.deployment["id"] != project
                or not re.fullmatch(r"[a-f0-9]{32}", self.deployment["incarnation"])):
            raise ValueError("invalid persistent deployment identity")
        self.names = state["names"]
        allowed = {"controller", "publisher", "gateway", "gateway2", "gateway3",
                   "collector1", "collector2", "collector3", "collector4"}
        if (not isinstance(self.names, dict) or not set(self.names) <= allowed
                or any(name != project + "-" + role for role, name in self.names.items())):
            raise ValueError("invalid deployment resource roles")
        self.control_role = state["control_role"]
        self.control_port = state["control_port"]
        if (self.control_role != ("controller" if arm == "adr" else "gateway")
                or self.control_role not in self.names or type(self.control_port) is not int
                or not 1 <= self.control_port <= 65535):
            raise ValueError("invalid deployment authority binding")
        self.sql_name = project + "-sql"
        self.records = state["containers"]
        expected = {*self.names.values(), self.sql_name, project + "-collector"}
        if not isinstance(self.records, dict) or set(self.records) != expected or len(expected) > 12:
            raise ValueError("invalid deployment ownership set")
        for name, record in self.records.items():
            if not isinstance(record, dict) or not re.fullmatch(r"[a-f0-9]{64}", record.get("id", "")):
                raise ValueError("invalid deployment resource identity")
            if describe(self.engine, name, record["id"], project) != record:
                raise ValueError("deployment resource incarnation changed")
        self.container_ids = {name: record["id"] for name, record in self.records.items()}
        if (self.root / "config").is_symlink() or (self.root / "bin").is_symlink():
            raise ValueError("invalid deployment artifact directory")
        binaries = state["binaries"]
        if not isinstance(binaries, dict) or not 1 <= len(binaries) <= 7:
            raise ValueError("invalid deployment binary manifest")
        for name, digest in binaries.items():
            qualified_binary(self.root, name, digest)
        self.config = read_private(self.root / "config" / (self.control_role + ".json"))
        if not isinstance(self.config, dict) or not self.config.get("operator_token"):
            raise ValueError("deployment authority configuration unavailable")

    def owned_container(self, name):
        if name not in self.container_ids:
            raise Denial("wrong-target")
        return self.container_ids[name]

    def post(self, operation):
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.control_port}/v1/authority",
            data=json.dumps(operation, allow_nan=False).encode(),
            headers={"Authorization": "Bearer " + self.config["operator_token"], "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(RESPONSE_BYTES + 1)
            if len(raw) > RESPONSE_BYTES:
                raise ValueError("authority response exceeds bound")
            return json.loads(raw, object_pairs_hook=unique_object)

    def finish_runtime(self):
        stopped = []
        for name, record in reversed(list(self.records.items())):
            if describe(self.engine, name, record["id"], self.project) != record:
                raise ValueError("refusing teardown of changed deployment incarnation")
            result = subprocess.run(["docker", "stop", "-t", "1", record["id"]],
                                    capture_output=True, timeout=20)
            if result.returncode != 0:
                raise RuntimeError("exact owned deployment cleanup failed")
            state = self.engine.get(f"/containers/{record['id']}/json")
            if state["State"]["Running"]:
                raise RuntimeError("exact owned deployment did not stop")
            stopped.append({"id": record["id"], "running": False})
        write_private_new(self.root / ("bridge-stopped-" + secrets.token_hex(8) + ".json"), stopped)
