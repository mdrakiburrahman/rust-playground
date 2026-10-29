"""Fresh bounded gap proofs; never regenerates completed benchmark/report cohorts."""
import argparse
from collections import Counter
import hashlib
import ipaddress
import json
from pathlib import Path
import subprocess
import socket
import time
import uuid

import benchmark as b


def run(project, baseline):
    output = b.REPO / "target/adr0001/evidence" / project
    output.mkdir(parents=True, exist_ok=False)
    sdk = b.sdks(baseline)[0]
    catalog = b.catalog(baseline)
    catalog += [{"key": f"reservation-{n}", "path": ["benchmark", "reservations", f"r{n}"],
                 "fingerprints": catalog[0]["fingerprints"]} for n in range(5)]
    root = b.adr.prepare(project, catalog)
    # Docker's dynamically assigned host port can change on start. Pin this exact
    # task's selected loopback port so retries really use the same gateway address.
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        gateway_port = reservation.getsockname()[1]
    spec = b.adr.yaml.safe_load((root / "compose.yaml").read_text())
    spec["services"]["gateway2"]["ports"] = [f"127.0.0.1:{gateway_port}:50051"]
    # Preserve previous projects' networks/state even when Docker's automatic
    # /16 pool is exhausted. Allocate only small nonoverlapping task networks.
    ids = subprocess.check_output(["docker", "--context", "default", "network", "ls", "-q"],
                                  text=True).split()
    existing = json.loads(subprocess.check_output(["docker", "--context", "default", "network", "inspect", *ids],
                                                  text=True))
    used = [ipaddress.ip_network(item["Subnet"]) for network in existing
            for item in (network.get("IPAM", {}).get("Config") or []) if item.get("Subnet")]
    used += [ipaddress.ip_network(route["dst"]) for route in
             json.loads(subprocess.check_output(["ip", "-j", "route"], text=True))
             if route["dst"] != "default"]
    candidates = list(ipaddress.ip_network("10.231.0.0/16").subnets(new_prefix=28))
    offset = int(hashlib.sha256(project.encode()).hexdigest()[:4], 16) % len(candidates)
    for name in spec["networks"]:
        for subnet in candidates[offset:] + candidates[:offset]:
            if not any(subnet.version == other.version and subnet.overlaps(other) for other in used):
                spec["networks"][name]["ipam"] = {"config": [{"subnet": str(subnet)}]}
                used.append(subnet)
                break
        else:
            raise RuntimeError("no nonoverlapping task subnet available; no cleanup attempted")
    (root / "compose.yaml").write_text(b.adr.yaml.safe_dump(spec, sort_keys=False))
    command = b.adr.compose(root)
    events = []

    def save(name, evidence):
        events.append({"scenario": name, "observed_ns": time.time_ns(), "evidence": evidence})
        (output / "events.json").write_text(json.dumps(events, indent=2))

    def call(op, role="operator"):
        return b.adr.control(root, op, role)

    def record(key):
        return next(a["record"] for a in call({"op": "view"})["assignments"] if a["record"]["key"] == key)

    def active(key):
        return b.adr.wait(root, lambda v: any(a["record"]["key"] == key
            and a["record"]["lifecycle"] == "Active" for a in v["assignments"]), seconds=40)

    def logs(service):
        return subprocess.check_output(command + ["logs", "--no-color", service], text=True,
                                       stderr=subprocess.STDOUT)

    def container(service):
        return subprocess.check_output(command + ["ps", "-q", service], text=True).strip()

    source = sorted((b.REPO / "crates/arrowcol-authority").glob("src/**/*.rs"))
    source += sorted((b.REPO / "crates/arrowcol-authority").glob("tests/*.rs"))
    source += sorted(Path(__file__).parent.glob("*.py"))
    source += [b.REPO / "Cargo.lock", b.REPO / "crates/arrowcol-authority/Cargo.toml"]
    provenance = {"project": project, "created_ns": time.time_ns(),
                  "source_sha256": {str(p.relative_to(b.REPO)): hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in source},
                  "binary_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                    for p in (root / "bin").iterdir()},
                  "previous_cohorts": "unchanged; these binaries are not the cohort-b binaries"}
    baseline_source = json.loads((b.REPO / "target/adr0001/evidence/baseline-source-after.json").read_text())
    original = Path(baseline).resolve().parents[2]
    provenance["baseline_manifest_verification"] = [
        {"path": row["path"], "matches": hashlib.sha256((original / row["path"]).read_bytes()).hexdigest() == row["sha256"]}
        for row in baseline_source]
    assert all(row["matches"] for row in provenance["baseline_manifest_verification"])
    provenance["images"] = json.loads(subprocess.check_output(
        ["docker", "--context", "default", "image", "inspect", b.adr.ETCD_IMAGE, b.adr.RUNTIME_IMAGE], text=True))
    # Retain IDs/digests, not unrelated image metadata.
    provenance["images"] = [{k: image.get(k) for k in ("Id", "RepoTags", "RepoDigests")}
                            for image in provenance["images"]]
    (output / "manifest.json").write_text(json.dumps(provenance, indent=2))
    try:
        b.adr.start(root)
        b.adr.wait(root, lambda v: len(v["members"]) == 2)
        subprocess.run(command + ["stop", "--timeout", "2", "collector2"], check=True)
        call({"op": "revoke", "collector": "collector2"})
        for item in catalog[-5:]:
            call({"op": "resolve", "key": item["key"]})
            active(item["key"])
        subprocess.run(command + ["up", "-d", "collector2", "collector3"], check=True)
        b.adr.wait(root, lambda v: len(v["members"]) == 3)
        subprocess.run(command + ["pause", "collector2", "collector3"], check=True)
        moves = [call({"op": "migrate", "key": f"reservation-{n}"}) for n in range(4)]
        try:
            call({"op": "migrate", "key": "reservation-4"})
        except RuntimeError as error:
            save("migration-reservation-global-budget", {"moves": moves, "rejected": str(error)})
        else:
            raise AssertionError("fifth concurrent reservation exceeded global budget")
        targets = Counter(m["record"]["migration"]["target"]["collector"] for m in moves)
        assert sorted(targets.values()) == [2, 2], targets
        # Controller restart cannot lose reservations held by paused recovery targets.
        subprocess.run(command + ["kill", "-s", "SIGKILL", "controller"], check=True)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            subprocess.run(command + ["start", "controller"], check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
            try:
                call({"op": "view"})
                break
            except RuntimeError:
                time.sleep(.5)
        try:
            call({"op": "migrate", "key": "reservation-4"})
        except RuntimeError as error:
            save("migration-reservations-survive-restart", {"rejected": str(error),
                 "targets": targets, "view": call({"op": "view"})})
        else:
            raise AssertionError("restart lost migration reservations")
        subprocess.run(command + ["unpause", "collector2", "collector3"], check=True)
        for n in range(4):
            active(f"reservation-{n}")
        call({"op": "migrate", "key": "reservation-4"})
        active("reservation-4")
        save("migration-completion-releases-budget", call({"op": "view"}))
        time.sleep(.6)
        old_move = record("reservation-4")
        call({"op": "admit", "key": "reservation-4", "writer": old_move["writer"],
              "epoch": old_move["epoch"], "unit": "hold-source"}, old_move["writer"]["collector"])
        failed_move = call({"op": "migrate", "key": "reservation-4"})["record"]
        failed_target = failed_move["migration"]["target"]["collector"]
        call({"op": "revoke", "collector": failed_target})
        active("reservation-4")
        canceled = record("reservation-4")
        assert canceled["writer"] == old_move["writer"] and canceled["migration"]["phase"] == "target_failed"
        save("failed-target-releases-reservation", canceled)
        subprocess.run(command + ["restart", failed_target], check=True)
        b.adr.wait(root, lambda v: len(v["members"]) == 3)

        subprocess.run(command + ["up", "-d", "gateway2"], check=True)
        config = json.loads((root / "secrets/controller.json").read_text())
        secret = config["edge_secret"].encode()
        settings = {"run_id": project, "rows": 8, "batches_per_stream": 4, "max_attempts": 4}
        address = b.adr.endpoint(root, "gateway2")
        gateway_id = container("gateway2")

        def remove_during_put(attempt, batch, receipt):
            if attempt == 1 and batch == 0:
                subprocess.run(command + ["kill", "-s", "SIGKILL", "gateway2"], check=True)
                # Same loopback address, same client unit; restore after the interruption.
                subprocess.run(command + ["start", "gateway2"], check=True)
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline:
                    try:
                        with socket.create_connection(("127.0.0.1", gateway_port), timeout=.2):
                            break
                    except OSError:
                        time.sleep(.1)
                else:
                    raise TimeoutError("same-address gateway did not return")

        outcome = b.retained_upload(address, secret, sdk, settings, 0,
                                    batch_observer=remove_during_put)
        save("gateway-removal-client-outcome", outcome)
        assert outcome["terminal"] == "ok" and len(outcome["attempts"]) > 1, outcome
        assert outcome["attempts"][0]["error"] is not None
        assert container("gateway2") == gateway_id
        markers, _ = b.visible(root)
        observed = Counter(m for values in markers.values() for m in values)
        assert not (Counter(outcome["markers"]) - observed)
        save("gateway-removal-during-persistent-doput", {"outcome": outcome,
             "missing_rows": 0, "duplicate_rows": sum(observed[m]-1 for m in outcome["markers"]),
             "stream_open_ms": b.distribution([a["stream_open_ms"] for a in outcome["attempts"]
                                               if "stream_open_ms" in a]),
             "admission": "unavailable: no provisional ADR admission receipt"})

        # The SAME live gateway is paused behind compaction and must resnapshot itself.
        gateway_id = container("gateway1")
        warm = b.retained_upload(address, secret, sdk, dict(settings, run_id=project+"-watch-warm",
                                                         batches_per_stream=1), 2)
        assert warm["terminal"] == "ok"
        warm_revision = warm["receipts"][-1]["revision"]
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if f'"assignment_revision":{warm_revision},' in logs("gateway1"):
                break
            time.sleep(.1)
        else:
            raise AssertionError("gateway did not consume pre-gap revision")
        before_log = logs("gateway1")
        subprocess.run(command + ["pause", "gateway1"], check=True)
        later = b.retained_upload(address, secret, sdk, dict(settings, run_id=project+"-gap"), 1)
        assert later["terminal"] == "ok"
        revision = call({"op": "view"})["revision"]
        b.adr.etcdctl(root, "compact", str(revision), authenticate=True)
        subprocess.run(command + ["unpause", "gateway1"], check=True)
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            delta = logs("gateway1")[len(before_log):]
            resets = [json.loads(line[line.index("{"):]) for line in delta.splitlines()
                      if '"event":"directory_watch_reset"' in line]
            if (any(int(reset["compact_revision"]) == revision for reset in resets)
                    and '"kind":"snapshot"' in delta):
                break
            time.sleep(.2)
        else:
            raise AssertionError("same live gateway did not resnapshot compacted watch")
        assert container("gateway1") == gateway_id
        save("same-live-gateway-compaction-resnapshot", {"container": gateway_id,
             "compacted_revision": revision, "log_after_pause": delta})

        # Pre-admit while connected, then prove an actual NEW object is created by the
        # partitioned collector with storage still mounted, without granting publication.
        key = b.table_key(sdk[0])
        old = record(key)
        unit = project + "-partition"
        call({"op": "admit", "key": key, "writer": old["writer"], "epoch": old["epoch"], "unit": unit},
             old["writer"]["collector"])
        collector = old["writer"]["collector"]
        cid = container(collector)
        networks = json.loads(subprocess.check_output(["docker", "inspect", cid], text=True))[0]["NetworkSettings"]["Networks"]
        network = next(n for n in networks if n.endswith("_coordination"))
        subprocess.run(["docker", "network", "disconnect", network, cid], check=True)
        try:
            previous = json.loads((root / "objects" / old["head"]["manifest"]).read_text())
            commit = str(uuid.uuid4())
            writer = old["writer"]
            prefix = f"{key}/{collector}/{writer['process']}/{writer['generation']}/{commit}"
            staged = prefix + ".parquet"
            subprocess.run(command + ["exec", "-T", collector, "mkdir", "-p",
                                     "/objects/"+str(Path(prefix).parent)], check=True)
            subprocess.run(command + ["exec", "-T", collector, "cp", "/objects/"+previous["parquet"],
                                     "/objects/"+staged], check=True)
            manifest = dict(previous, commit=commit, unit=unit, parent=old["head"], parquet=staged,
                            writer=writer, epoch=old["epoch"])
            raw = json.dumps(manifest).encode()
            subprocess.run(command + ["exec", "-T", collector, "tee", "/objects/"+prefix+".json"],
                           input=raw, stdout=subprocess.DEVNULL, check=True)
            proposal = {"commit": commit, "manifest": prefix+".json", "sha256": hashlib.sha256(raw).hexdigest(),
                        "sequence": old["head"]["sequence"]+1}
            b.adr.wait(root, lambda v: any(a["record"]["key"] == key and a["record"]["epoch"] > old["epoch"]
                and a["record"]["lifecycle"] == "Active" for a in v["assignments"]), seconds=45)
            new_head = record(key)["head"]
            try:
                call({"op": "commit", "key": key, "writer": writer, "epoch": old["epoch"],
                      "head": proposal}, collector)
            except RuntimeError as error:
                assert record(key)["head"] == new_head
                save("partition-new-staged-object-no-publication", {"before": old, "after": record(key),
                    "new_object": staged, "proposed": proposal,
                    "sha256": hashlib.sha256((root/"objects"/staged).read_bytes()).hexdigest(),
                    "rejection": str(error)})
            else:
                raise AssertionError("expired writer admitted after partition")
        finally:
            subprocess.run(["docker", "network", "connect", "--alias", collector, network, cid], check=True)
        current = record(key)
        old_writer = current["writer"]
        pending_unit = project+"-replacement-pending"
        call({"op": "admit", "key": key, "writer": old_writer, "epoch": current["epoch"],
              "unit": pending_unit}, old_writer["collector"])
        prior = json.loads((root / "objects" / current["head"]["manifest"]).read_text())
        replacement_commit = str(uuid.uuid4())
        prefix = f"{key}/{old_writer['collector']}/{old_writer['process']}/{old_writer['generation']}/{replacement_commit}"
        raw = json.dumps(dict(prior, writer=old_writer, epoch=current["epoch"], commit=replacement_commit,
                              unit=pending_unit, parent=current["head"], parquet=prefix+".parquet")).encode()
        owner = old_writer["collector"]
        subprocess.run(command + ["exec", "-T", owner, "mkdir", "-p",
                                 "/objects/"+str(Path(prefix).parent)], check=True)
        subprocess.run(command + ["exec", "-T", owner, "cp", "/objects/"+prior["parquet"],
                                 "/objects/"+prefix+".parquet"], check=True)
        (root / "objects" / (prefix+".json")).write_bytes(raw)
        proposed = {"commit": replacement_commit, "manifest": prefix+".json",
                    "sha256": hashlib.sha256(raw).hexdigest(), "sequence": current["head"]["sequence"]+1}
        rotated = call({"op": "replace_session", "writer": old_writer}, owner)
        successor = record(key)
        assert successor["epoch"] > current["epoch"] and successor["writer"] == rotated["writer"]
        assert successor["writer"]["generation"] != old_writer["generation"]
        for operation in (
            {"op": "admit", "key": key, "writer": old_writer, "epoch": current["epoch"], "unit": "late-old-runtime"},
            {"op": "commit", "key": key, "writer": old_writer, "epoch": current["epoch"], "head": proposed},
        ):
            try:
                call(operation, owner)
            except RuntimeError as error:
                save("replaced-local-generation-"+operation["op"]+"-rejected",
                     {"operation": operation, "error": str(error), "successor": successor})
            else:
                raise AssertionError("replaced generation regained authority")
        assert record(key)["head"] == current["head"]
    finally:
        # Preserve state, volumes and failed logs; no global lifecycle commands.
        subprocess.run(command + ["unpause", "collector2", "collector3", "gateway1"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        (output / "runtime.log").write_text(subprocess.run(command + ["logs", "--no-color"],
            text=True, capture_output=True).stdout)
        subprocess.run(command + ["--profile", "*", "stop", "--timeout", "5"], check=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--baseline", required=True)
    args = parser.parse_args()
    print(run(args.project, args.baseline))
