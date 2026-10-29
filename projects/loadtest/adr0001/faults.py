"""Real task-scoped authority races, process/network failures and reader reconciliation.

Assertions are on authoritative heads and live state, never elapsed sleeps alone.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import benchmark as b


class Experiments:
    def __init__(self, root, baseline):
        self.root = Path(root)
        self.baseline = baseline
        self.command = b.adr.compose(root)
        self.sdk = b.sdks(baseline)[0]
        self.key = b.table_key(self.sdk[0])
        self.output = b.REPO / "target/adr0001/evidence/faults"
        self.output.mkdir(parents=True, exist_ok=False)
        self.events = []
        self.config = json.loads((self.root / "secrets/controller.json").read_text())

    def save(self, name, value):
        self.events.append({"scenario": name, "observed_ns": time.time_ns(), "evidence": value})
        (self.output / "events.json").write_text(json.dumps(self.events, indent=2))

    def view(self):
        return b.adr.control(self.root, {"op": "view"})

    def record(self):
        return next(a["record"] for a in self.view()["assignments"] if a["record"]["key"] == self.key)

    def call(self, operation, writer=None):
        return b.adr.control(self.root, operation, writer["collector"] if writer else "operator")

    def stage(self, record, suffix):
        unit = str(uuid.uuid4())
        admitted = self.call({"op": "admit", "key": self.key, "writer": record["writer"],
                             "epoch": record["epoch"], "unit": unit}, record["writer"])["record"]
        batch = b.native.make_batch(self.sdk[1], 4, suffix)
        commit = str(uuid.uuid4())
        writer = record["writer"]
        prefix = f"{self.key}/{writer['collector']}/{writer['process']}/{writer['generation']}/{commit}"
        destination = self.root / "objects" / (prefix + ".parquet")
        destination.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_batches([batch]), destination)
        # Fault injector deliberately stages outside the normal writer to pause AFTER admission/precheck.
        # The tested publication fence still validates finalized content and cannot trust this process.
        manifest = {"table": self.key, "writer": writer, "epoch": record["epoch"],
                    "commit": commit, "unit": unit, "parent": admitted["head"],
                    "parquet": prefix + ".parquet",
                    "parquet_sha256": hashlib.sha256(destination.read_bytes()).hexdigest(),
                    "rows": 4, "arrow_bytes": batch.nbytes, "fingerprint": self.sdk[0]["table"]["fingerprint"]}
        raw = json.dumps(manifest).encode()
        (self.root / "objects" / (prefix + ".json")).write_bytes(raw)
        head = {"commit": commit, "manifest": prefix + ".json", "sha256": hashlib.sha256(raw).hexdigest(),
                "sequence": 1 + (admitted["head"]["sequence"] if admitted["head"] else 0)}
        return {"op": "commit", "key": self.key, "writer": writer, "epoch": record["epoch"], "head": head}

    def rejected(self, operation, writer, name):
        before = self.record()["head"]
        try:
            self.call(operation, writer)
        except RuntimeError as error:
            after = self.record()["head"]
            assert before == after, "rejected stale publication changed authoritative head"
            self.save(name, {"rejected": True, "error": str(error), "before": before, "after": after,
                             "proposed_head": operation.get("head")})
        else:
            raise AssertionError(f"{name} incorrectly succeeded")

    def migrate(self):
        old = self.record()
        op = self.call({"op": "migrate", "key": self.key})
        view = b.adr.wait(self.root, lambda v: any(a["record"]["key"] == self.key
            and a["record"]["epoch"] > old["epoch"] and a["record"]["lifecycle"] == "Active"
            for a in v["assignments"]))
        record = next(a["record"] for a in view["assignments"] if a["record"]["key"] == self.key)
        assert record["epoch"] > old["epoch"]
        self.save("migration", {"before": old, "operation": op, "after": record})
        return record

    def restart_controller(self):
        subprocess.run(self.command + ["kill", "-s", "SIGKILL", "controller"], check=True)
        # Readiness is proved by authenticated authoritative reads, not by this bounded lease wait.
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            subprocess.run(self.command + ["start", "controller"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                return b.adr.wait(self.root, lambda v: True, seconds=1)
            except TimeoutError:
                time.sleep(0.5)
        raise TimeoutError("controller could not resume durable authority")

    def run(self):
        record = self.record()
        # A paused after authority admission/precheck, B activates, A resumes with storage still accessible.
        pending = self.stage(record, "stale-after-precheck")
        new = self.migrate()
        self.rejected(pending, record["writer"], "paused-A-after-B-activation")
        # A caller cannot substitute B's writer using A's process credential.
        forged = dict(pending, writer=new["writer"], epoch=new["epoch"])
        self.rejected(forged, record["writer"], "writer-impersonation")
        # Exact proposal repeated after success resolves the same durable commit, not a second append.
        valid = self.stage(new, "unknown-commit-identity")
        first = self.call(valid, new["writer"])
        second = self.call(valid, new["writer"])
        assert first["head"] == second["head"]
        self.save("exact-proposal-reconciliation", {"first": first, "second": second})
        # Initial stale-writer proof above uses exactly two collectors and two gateways.
        subprocess.run(self.command + ["up", "-d", "--no-deps", "collector3"], check=True)
        b.adr.wait(self.root, lambda v: len(v["members"]) == 3)
        # Restart in ACTIVE, then DRAINING with an admitted operation that never finishes.
        before = self.record()
        self.restart_controller()
        assert self.record()["head"] == before["head"]
        self.save("controller-restart-active", {"before": before, "after": self.record()})
        record = self.record()
        pending = self.stage(record, "restart-draining-stale")
        operation = self.call({"op": "migrate", "key": self.key})
        assert operation["record"]["lifecycle"] == "Draining"
        self.restart_controller()
        b.adr.wait(self.root, lambda v: any(a["record"]["key"] == self.key
            and a["record"]["epoch"] > record["epoch"] and a["record"]["lifecycle"] == "Active" for a in v["assignments"]))
        self.rejected(pending, record["writer"], "controller-restart-draining-stale-rejected")
        self.save("controller-restart-draining", {"operation": operation, "after": self.record()})
        record = self.record()
        pending = self.stage(record, "recovering-target-failure")
        operation = self.call({"op": "migrate", "key": self.key})
        target = operation["record"]["migration"]["target"]["collector"]
        subprocess.run(self.command + ["pause", target], check=True)
        try:
            recovering = b.adr.wait(self.root, lambda v: any(a["record"]["key"] == self.key
                and a["record"]["lifecycle"] == "Recovering" and a["record"]["epoch"] > record["epoch"]
                for a in v["assignments"]))
            self.restart_controller()
            b.adr.wait(self.root, lambda v: any(a["record"]["key"] == self.key and a["record"]["lifecycle"] == "Active"
                and a["record"]["writer"]["collector"] != target for a in v["assignments"]), seconds=30)
            self.rejected(pending, record["writer"], "recovering-restart-target-death-stale-source")
            self.save("controller-restart-recovering-target-failure", {"recovering": recovering, "after": self.record()})
        finally:
            subprocess.run(self.command + ["unpause", target], check=True)
            subprocess.run(self.command + ["restart", target], check=True)
        b.adr.wait(self.root, lambda v: len(v["members"]) >= 3,seconds=40)
        # Disconnect only the owner's coordination link. Data network and bind-mounted storage remain.
        record = self.record()
        pending = self.stage(record, "partition-orphan")
        collector = record["writer"]["collector"]
        container = subprocess.check_output(self.command + ["ps", "-q", collector], text=True).strip()
        networks = json.loads(subprocess.check_output(["docker", "inspect", container], text=True))[0]["NetworkSettings"]["Networks"]
        network = next(name for name in networks if name.endswith("_coordination"))
        subprocess.run(["docker", "network", "disconnect", network, container], check=True)
        try:
            b.adr.wait(self.root, lambda v: any(a["record"]["key"] == self.key
                and a["record"]["epoch"] > record["epoch"] and a["record"]["lifecycle"] == "Active" for a in v["assignments"]), seconds=35)
            assert (self.root / "objects" / pending["head"]["manifest"]).is_file()
            self.rejected(pending, record["writer"], "partitioned-A-storage-reachable-no-publication")
        finally:
            subprocess.run(["docker", "network", "connect", "--alias", collector, network, container], check=True)
        # Expired incarnation must not resurrect; replace only this task collector with a new process.
        time.sleep(0.5)
        try:
            member = next(m for m in self.view()["members"] if m["writer"] == record["writer"])
        except StopIteration:
            member = None
        assert member is None, "expired process generation resurrected after reconnect"
        self.save("expired-session-no-resurrection", {"writer": record["writer"]})
        subprocess.run(self.command + ["restart", collector], check=True)
        b.adr.wait(self.root, lambda v: len(v["members"]) >= 3)
        # Kill current owner; a different epoch must become active without relying on process cooperation.
        record = self.record()
        dead = record["writer"]["collector"]
        subprocess.run(self.command + ["kill", "-s", "SIGKILL", dead], check=True)
        b.adr.wait(self.root, lambda v: any(a["record"]["key"] == self.key
            and a["record"]["epoch"] > record["epoch"] and a["record"]["lifecycle"] == "Active" for a in v["assignments"]), seconds=35)
        self.save("owner-death", {"before": record, "after": self.record()})
        subprocess.run(self.command + ["start", dead], check=True)
        b.adr.wait(self.root, lambda v: len(v["members"]) >= 3)
        # Recover a compacted watch cursor from an authoritative snapshot.
        revision = self.view()["revision"]
        b.adr.etcdctl(self.root, "compact", str(revision), authenticate=True)
        compacted = b.adr.control(self.root, {"op": "watch", "after": 1}, "gateway")
        assert compacted["reset"] is True, "compacted cursor did not require authoritative resnapshot"
        subprocess.run(self.command + ["restart", "gateway1"], check=True)
        self.save("watch-compaction", {"compacted_revision": revision, "response": compacted, "snapshot": self.view()})
        # One member stopped retains quorum; two stopped must fail all new admissions and commits closed.
        record = self.record()
        valid = self.stage(record, "one-member-loss")
        subprocess.run(self.command + ["stop", "--timeout", "2", "etcd3"], check=True)
        self.save("one-etcd-member-loss", self.call(valid, record["writer"]))
        pending = self.stage(self.record(), "quorum-loss-orphan")
        subprocess.run(self.command + ["stop", "--timeout", "2", "etcd2"], check=True)
        try:
            try:
                self.call(pending, pending["writer"])
            except (RuntimeError, subprocess.CalledProcessError) as error:
                self.save("quorum-loss-fail-closed", {"error": str(error), "proposed": pending["head"]})
            else:
                raise AssertionError("commit succeeded without etcd quorum")
        finally:
            subprocess.run(self.command + ["start", "etcd2", "etcd3"], check=True)
        self.restart_controller()
        old_processes = {m["writer"]["process"] for m in self.view()["members"]}
        subprocess.run(self.command + ["restart", "collector1", "collector2", "collector3"], check=True)
        b.adr.wait(self.root, lambda v: len(v["members"]) == 3
            and all(m["writer"]["process"] not in old_processes for m in v["members"])
            and all(a["record"]["lifecycle"] == "Active" for a in v["assignments"]), seconds=45)
        # If the control lease expired, a new control process must acquire truth, never reset heads.
        old_head = pending["head"]
        current = self.record()
        assert current["head"] != old_head, "unconfirmed quorum-loss proposal became visible"
        self.save("quorum-restored", current)
        recovered = b.run("adr", self.root, self.baseline, "fault-recovery", indexes=[0], streams=3, max_attempts=4)
        assert recovered["reconciled"], "post-quorum retained upload did not recover"
        markers, manifests = b.visible(self.root)
        # Independently reconcile all successful client markers captured in this proof dataset.
        observed = {marker for table in markers.values() for marker in table}
        expected = set()
        for label in ("invariant-initial","fault-recovery"):
            stream_file=b.REPO/"target/adr0001/evidence/adr"/label/"streams.json"
            for outcome in json.loads(stream_file.read_text()):
                if outcome["terminal"]=="ok":
                    expected.update(outcome["markers"])
        for outcome in json.loads((b.REPO/"target/adr0001/evidence/schema-proof.json").read_text())["outcomes"]:
            expected.update(outcome["markers"])
        missing=expected-observed
        assert not missing, f"acknowledged markers absent: {len(missing)}"
        self.save("authoritative-output", {"tables": len(markers), "rows": sum(len(m) for m in markers.values()),
                                         "manifest_count": len(manifests), "acknowledged_unique_markers":len(expected),
                                         "missing_acknowledged_rows": len(missing)})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--baseline", required=True)
    args = parser.parse_args()
    Experiments(args.root, args.baseline).run()
