"""One finite collector3-to-4 adapter; removal and fresh-identity reuse are unqualified."""
import hashlib
from concurrent.futures import ThreadPoolExecutor
import os
import re
import subprocess

from live_contract import Denial, SNAPSHOT_AGE_NS, validate_packet
from live_deployment import read_private, verified_state, write_private_new
from live_inventory import creation_labels, creation_state, read_inventory, serialized
from publication_gate import RUNTIME_IMAGE


class CollectorCapacity:
    def __init__(self, runtime, inventory, snapshot):
        self.runtime, self.inventory, self.snapshot = runtime, inventory, snapshot

    @staticmethod
    def validate_action(action):
        if action != {"operation": "replicas", "role": "collector", "desired": 4}:
            raise Denial("unsupported")

    def item(self, operation_id):
        item = next((item for item in read_inventory(self.runtime.root, self.runtime.deployment)
                     if item["operation_id"] == operation_id), None)
        if item is None:
            raise Denial("unknown-operation")
        return item

    def available(self):
        return not any(slot["role"] == "collector" and slot["slot"] == 4 and slot["record"] is not None
                       for slot in self.inventory.snapshot()["slots"])

    def prepare(self, operation_id):
        runtime = self.runtime
        runtime.refresh_inventory()
        if runtime.arm != "adr" or set(name for name in runtime.names if name.startswith("collector")) != {
            "collector1", "collector2", "collector3"
        }:
            raise Denial("unsupported")
        native = runtime.post({"op": "view"})
        members = {member["writer"]["collector"]: member for member in native["members"]}
        if set(members) != {"collector1", "collector2", "collector3"} or any(
            member["eligible"] is not True for member in members.values()
        ):
            raise Denial("disconnected")
        if "collector4" in native.get("policies", {}):
            raise Denial("unsupported")
        if any(record["record"]["lifecycle"] != "Active" for record in native["assignments"]):
            raise Denial("disconnected")
        with ThreadPoolExecutor(max_workers=3) as pool:
            states = list(pool.map(lambda role: verified_state(runtime, runtime.names[role]), members))
        for state in states:
            if state["State"].get("Running") is not True or state["State"].get("Paused") is not False:
                raise Denial("disconnected")
        config = read_private(runtime.root / "config" / "collector4.json")
        if (config.get("operator_token") or config.get("gateway_token") or config.get("etcd")
                or config.get("etcd_password") or config.get("storage") != "/stage"
                or config.get("collectors") != {"collector4": runtime.config["collectors"]["collector4"]}
                or config.get("tables") != runtime.config["tables"]
                or config.get("delta") != {"publisher": "http://publisher:50061"}):
            raise Denial("wrong-target")
        item = self.item(operation_id)
        pinned = runtime.root / "config" / ("bridge-collector4-" + item["nonce"] + ".json")
        if not pinned.exists():
            write_private_new(pinned, config)
            pinned.chmod(0o444)
            descriptor = os.open(pinned, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        if read_private(pinned) != config:
            raise Denial("wrong-target")
        stage = runtime.root / "staging" / "collector4"
        if stage.is_symlink():
            raise Denial("wrong-target")
        stage.mkdir(exist_ok=True)
        stage.chmod(0o777)
        labels = creation_labels(runtime.deployment, item["nonce"])
        command = ["docker", "run", "--pull=never", "-d", "--name", item["name"]]
        for key, value in labels.items():
            command += ["--label", key + "=" + value]
        command += [
            "--network", runtime.project, "--network-alias", "collector4",
            "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
            "--user", "21001:21001", "--cpus", "1", "--memory", "1g", "--pids-limit", "256",
            "-e", "ARROWCOL_TRACE=1",
            "--mount", f"type=bind,src={runtime.root / 'bin'},dst=/native,readonly",
            "--mount", f"type=bind,src={pinned},dst=/config.json,readonly",
            "--mount", f"type=bind,src={stage},dst=/stage",
            "--mount", f"type=bind,src={runtime.root / 'delta-private/tables'},dst=/destination,readonly",
            RUNTIME_IMAGE, "/native/adr-flight", "--config", "/config.json", "--collector", "collector4",
        ]
        return command, {"path": pinned.name, "sha256": hashlib.sha256(pinned.read_bytes()).hexdigest()}

    def create(self, command):
        result = subprocess.run(command, capture_output=True, timeout=20)
        if result.returncode != 0 or not re.fullmatch(rb"[a-f0-9]{64}\s*", result.stdout):
            raise RuntimeError("collector creation outcome unknown; reconcile same identity")

    def run(self, operation_id, action, authorize, admitted):
        self.validate_action(action)
        revision = self.inventory.snapshot()["revision"]
        fresh = self.inventory.reserve_creation(operation_id, "collector", 4, revision)
        if not fresh:
            return self.outcome(operation_id)
        try:
            command, config = self.prepare(operation_id)
            authorize()
        except Denial:
            self.inventory.cancel_preflight(operation_id)
            raise
        if self.inventory.begin_creation(operation_id, self.inventory.snapshot()["revision"], config):
            admitted()
            self.create(command)
        return self.outcome(operation_id)

    def isolation(self, item, state):
        runtime = self.runtime
        host = state["HostConfig"]
        if (host.get("NetworkMode") != runtime.project or host.get("Privileged") is not False
                or host.get("ReadonlyRootfs") is not True or host.get("NanoCpus") != 1_000_000_000
                or host.get("Memory") != 1_073_741_824 or host.get("PidsLimit") != 256
                or set(host.get("CapDrop") or []) != {"ALL"}
                or "no-new-privileges" not in (host.get("SecurityOpt") or [])
                or host.get("PortBindings") not in (None, {})
                or host.get("PidMode", "") not in ("", "private")
                or host.get("IpcMode", "") not in ("", "private")
                or set(state["NetworkSettings"]["Networks"]) != {runtime.project}
                or "collector4" not in state["NetworkSettings"]["Networks"][runtime.project].get("Aliases", [])
                or state["Config"].get("User") != "21001:21001"
                or state["Config"].get("Cmd") != [
                    "/native/adr-flight", "--config", "/config.json", "--collector", "collector4"]):
            raise Denial("wrong-target")
        config = item["dispatch"]
        if config is None or not re.fullmatch(r"bridge-collector4-[a-f0-9]{64}[.]json", config["path"]):
            raise Denial("wrong-target")
        path = runtime.root / "config" / config["path"]
        if path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != config["sha256"]:
            raise Denial("wrong-target")
        expected = {
            "/native": (str(runtime.root / "bin"), False),
            "/config.json": (str(path), False),
            "/stage": (str(runtime.root / "staging/collector4"), True),
            "/destination": (str(runtime.root / "delta-private/tables"), False),
        }
        mounts = {mount["Destination"]: mount for mount in state["Mounts"]}
        if set(mounts) != set(expected) or any(
            mounts[destination]["Type"] != "bind" or mounts[destination]["Source"] != source
            or mounts[destination]["RW"] is not writable for destination, (source, writable) in expected.items()
        ):
            raise Denial("wrong-target")

    def outcome(self, operation_id):
        item = self.item(operation_id)
        if item["dispatch"] is None:
            return "unknown", None
        state = creation_state(self.runtime.engine, self.runtime.deployment, item, require_running=True)
        if state is None:
            return "unknown", None
        self.isolation(item, state)
        self.inventory.bind_creation(operation_id, self.runtime.engine, self.inventory.snapshot()["revision"])
        self.runtime.refresh_inventory()
        native = self.runtime.post({"op": "view"})
        member = next((member for member in native["members"]
                       if member["writer"]["collector"] == "collector4"), None)
        if (member is None or member["eligible"] is not True
                or "collector4" not in native.get("observations", {})):
            return "pending", None
        snapshot = validate_packet(self.snapshot())
        freshness = snapshot["freshness"]
        resource = {"id": "resource-collector4", "incarnation": hashlib.sha256(
            (state["Id"] + state["State"]["StartedAt"]).encode()).hexdigest()}
        replica = next((row for row in snapshot["replicas"] if row["resource"] == resource), None)
        if (snapshot["deployment"] != self.runtime.deployment or freshness["status"] != "fresh"
                or freshness["age_ns"] is None or not 0 <= int(freshness["age_ns"]) <= SNAPSHOT_AGE_NS
                or replica is None
                or replica["identity_verified"] is not True or replica["running"] is not True
                or replica["paused"] is not False or replica["status"] != "active"
                or replica["writer"] != {**member["writer"], "generation": str(member["writer"]["generation"])}):
            return "pending", None
        return "completed", snapshot["revision"]
