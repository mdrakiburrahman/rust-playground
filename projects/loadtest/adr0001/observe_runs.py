"""Fresh, sequential whole-architecture observations, separate from the original cohort."""
import argparse
import hashlib
import ipaddress
import json
from pathlib import Path
import subprocess
import sys
import time
import benchmark as b
from observation import Observer


def main(run_id):
    if not run_id.replace("-", "").isalnum():
        raise ValueError("unsafe observation run ID")
    cohort = "instrumented-" + run_id
    evidence = b.REPO / "target/adr0001/evidence" / cohort
    evidence.mkdir(parents=True, exist_ok=False)
    sources = [*sorted((b.REPO/"crates/arrowcol-authority").rglob("*.rs")),
               b.REPO/"crates/arrowcol-authority/Cargo.toml", b.REPO/"Cargo.lock",
               *sorted(Path(__file__).parent.glob("*.py"))]
    source_hashes = {str(p.relative_to(b.REPO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}
    state = {"cohort": cohort, "command": sys.argv, "cwd": str(b.REPO), "source_sha256": source_hashes,
             "binary_sha256": {name: hashlib.sha256((b.REPO/"target/debug"/name).read_bytes()).hexdigest()
                               for name in ("adr-controller", "adr-flight", "adr-fingerprint")},
             "roots": {}, "events": []}
    def event(design, kind, value):
        state["events"].append({"design": design, "kind": kind, "observed_ns": time.time_ns(), "value": value})
        (evidence/"run.json").write_text(json.dumps(state, indent=2))
    first = Path((b.REPO/"target/adr0001/evidence/fresh-baseline-root.txt").read_text().strip())
    original = b.baseline_module(first)
    ids = subprocess.check_output(["docker", "--context", "default", "network", "ls", "-q"],
                                  text=True).split()
    networks = json.loads(subprocess.check_output(
        ["docker", "--context", "default", "network", "inspect", *ids], text=True))
    used = [ipaddress.ip_network(item["Subnet"]) for network in networks
            for item in (network.get("IPAM", {}).get("Config") or []) if item.get("Subnet")]
    used += [ipaddress.ip_network(route["dst"]) for route in
             json.loads(subprocess.check_output(["ip", "-j", "route"], text=True))
             if route["dst"] != "default"]
    subnets = []
    for candidate in ipaddress.ip_network("10.232.0.0/16").subnets(new_prefix=24):
        if not any(candidate.version == other.version and candidate.overlaps(other) for other in used):
            subnets.append(str(candidate))
            used.append(candidate)
            if len(subnets) == 4:
                break
    if len(subnets) != 4:
        raise RuntimeError("no nonoverlapping task subnets available; no cleanup attempted")
    state["explicit_subnets"] = subnets
    event("both", "network-allocation", subnets)
    subnet = subnets[0]
    baseline = original.prepare("pingora-bench-adr"+run_id, subnet, "least_pressure")
    state["roots"]["baseline"] = str(baseline)
    command = original.context(baseline)[1]
    for image in ("runtime", "sqlserver", "hive"):
        subprocess.run(["docker", "--context", "default", "tag", f"{first.name}/{image}:local",
                        f"{baseline.name}/{image}:local"], check=True)

    def stop(cmd):
        subprocess.run(cmd+["--profile", "*", "stop", "--timeout", "5"], check=True)

    def run(design, root, observer, label, **kwargs):
        result = b.run(design, root, baseline, label, cohort=cohort, observer=observer,
                       streams=24, rows=64, batches=3, pacing_ms=100, **kwargs)
        assert result["reconciled"] and result["unresolved_streams"] == 0, label
        assert result["generated_rows"] == result["visible_rows"] and result["missing_rows"] == 0, label
        return result

    for design in ("baseline", "adr"):
        if design == "adr":
            root = b.adr.prepare("adr0001-"+run_id, b.catalog(baseline))
            spec = b.adr.yaml.safe_load((root/"compose.yaml").read_text())
            for name, subnet in zip(spec["networks"], subnets[1:], strict=True):
                spec["networks"][name]["ipam"] = {"config": [{"subnet": subnet}]}
            (root/"compose.yaml").write_text(b.adr.yaml.safe_dump(spec, sort_keys=False))
            state["roots"]["adr"] = str(root)
            command = b.adr.compose(root)
        else:
            root = baseline
        observer = None
        try:
            if design == "baseline":
                subprocess.run(command+["up", "-d", "--no-build", "--wait", "--wait-timeout", "120",
                                       "router", "direct-1", "direct-2", "direct-3"], check=True)
                subprocess.run(command+["run", "--rm", "--no-deps", "prepare"], check=True)
                def view():
                    return original.control(root, "view")["value"]
                def probe():
                    current = view()
                    return {"observed_ns": time.time_ns(), "collectors": [
                        {"id": c["registration"]["instance_id"], "service_key": c["registration"]["instance_key"],
                         "pressure": c.get("pressure")} for c in current["collectors"]]}
            else:
                b.adr.start(root)
                subprocess.run(command+["up", "-d", "--no-deps", "collector3"], check=True)
                b.adr.wait(root, lambda v: len(v["members"]) == 3)
                def view():
                    return b.adr.control(root, {"op": "view"})
                def probe():
                    current = view()
                    return {"observed_ns": time.time_ns(), "directory_revision": current["revision"],
                        "assignments": [{"key": a["record"]["key"], "revision": a["revision"],
                            "epoch": a["record"]["epoch"], "lifecycle": a["record"]["lifecycle"],
                            "pending_units": len(a["record"]["pending"])} for a in current["assignments"]],
                        "members": current["members"]}
            observer = Observer(root.name, evidence/"observations"/design, probe=probe).start()
            run(design, root, observer, "cold")
            for n in range(1, 4):
                run(design, root, observer, f"warm-{n:02}")
            service = "direct-1" if design == "baseline" else "collector1"
            stress = subprocess.Popen(command+["exec", "-T", service, "timeout", "18", "sh", "-c",
                "while :; do :; done"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                observer.phase("pressure-establishment")
                time.sleep(2)
                event(design, "pressure", view())
                observer.finish("pressure-establishment")
                run(design, root, observer, "pressure", indexes=[3, 4, 5])
            finally:
                stress.wait(timeout=22)
            run(design, root, observer, "elephant", indexes=[0])

            def move(key):
                if design == "baseline":
                    return original.move(root, key)
                old = next(a["record"] for a in view()["assignments"] if a["record"]["key"] == key)
                operation = b.adr.control(root, {"op": "migrate", "key": key})
                current = b.adr.wait(root, lambda v: any(a["record"]["key"] == key
                    and a["record"]["epoch"] > old["epoch"] and a["record"]["lifecycle"] == "Active"
                    for a in v["assignments"]))
                return {"operation": operation, "after": next(a["record"] for a in current["assignments"]
                                                              if a["record"]["key"] == key)}

            observer.phase("migration")
            current = view()
            if design == "baseline":
                lease = current["leases"][0]
                owner = next(c for c in current["collectors"]
                             if c["registration"]["instance_id"] == lease["holder_instance_id"])
                service = "direct-" + owner["registration"]["instance_key"].rsplit("-", 1)[1]
                key = lease["key"]
            else:
                key = current["assignments"][0]["record"]["key"]
                service = current["assignments"][0]["record"]["writer"]["collector"]
            pressure = subprocess.Popen(command+["exec", "-T", service, "timeout", "20", "sh", "-c",
                "while :; do :; done"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                deadline = time.monotonic()+12
                while time.monotonic() < deadline:
                    current = view()
                    if design == "baseline":
                        busy = next(c for c in current["collectors"] if
                                    c["registration"]["instance_id"] == lease["holder_instance_id"])
                        ready = busy["pressure"]["cpu_utilization_bps"] > 2500
                    else:
                        ready = any(m["writer"]["collector"] == service and m["pressure"] > .5 for m in current["members"])
                    if ready:
                        break
                    time.sleep(.25)
                else:
                    raise AssertionError("measured migration pressure missing")
                event(design, "migration", move(key))
            finally:
                pressure.wait(timeout=25)
            observer.finish("migration")
            run(design, root, observer, "after-move")
            observer.phase("scale-out")
            fourth = "direct-4" if design == "baseline" else "collector4"
            subprocess.run(command+["up", "-d", "--no-deps", fourth], check=True)
            if design == "adr":
                b.adr.wait(root, lambda v: len(v["members"]) == 4)
            else:
                deadline = time.monotonic()+15
                while len(view()["collectors"]) < 4 and time.monotonic() < deadline:
                    time.sleep(.25)
                assert len(view()["collectors"]) == 4
            event(design, "four-collectors", view())
            observer.finish("scale-out")
            run(design, root, observer, "four-new", indexes=[9, 10, 11])
            observer.phase("inbound-fourth-and-evacuation")
            pressures = [subprocess.Popen(command+["exec", "-T",
                f"direct-{n}" if design == "baseline" else f"collector{n}", "timeout", "20", "sh", "-c",
                "while :; do :; done"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for n in (1, 2, 3)]
            try:
                deadline = time.monotonic()+14
                while time.monotonic() < deadline:
                    current = view()
                    if design == "baseline":
                        busy = [c for c in current["collectors"] if
                                c["registration"]["instance_key"] != "routing-benchmark-direct-4"]
                        ready = len(busy) == 3 and all(c["pressure"]["cpu_utilization_bps"] > 2500 for c in busy)
                    else:
                        ready = all(m["pressure"] > .5 for m in current["members"] if
                                    m["writer"]["collector"] != "collector4")
                    if ready:
                        break
                    time.sleep(.25)
                else:
                    raise AssertionError("measured inbound pressure missing")
                if design == "baseline":
                    target = next(c for c in current["collectors"] if
                                  c["registration"]["instance_key"] == "routing-benchmark-direct-4")
                    key = next(l["key"] for l in current["leases"] if
                               l["holder_instance_id"] != target["registration"]["instance_id"])
                    moved = move(key)
                    assert moved["lease"]["holder_instance_id"] == target["registration"]["instance_id"]
                else:
                    key = next(a["record"]["key"] for a in current["assignments"] if
                               a["record"]["writer"]["collector"] != "collector4")
                    moved = move(key)
                    assert moved["after"]["writer"]["collector"] == "collector4"
                event(design, "inbound-fourth", moved)
            finally:
                for process in pressures:
                    process.wait(timeout=25)
            if design == "baseline":
                event(design, "evacuation", original.scale_in(root))
            else:
                b.adr.control(root, {"op": "drain_collector", "collector": "collector4"})
                keys = [a["record"]["key"] for a in view()["assignments"] if
                        a["record"]["writer"]["collector"] == "collector4"]
                for key in keys:
                    event(design, "evacuation", move(key))
                b.adr.wait(root, lambda v: all(a["record"]["writer"]["collector"] != "collector4"
                    and a["record"]["lifecycle"] == "Active" for a in v["assignments"]))
                subprocess.run(command+["stop", "--timeout", "5", "collector4"], check=True)
                b.adr.control(root, {"op": "revoke", "collector": "collector4"})
            observer.finish("inbound-fourth-and-evacuation")
            run(design, root, observer, "after-scale-in", indexes=[9, 10, 11])
            observer.phase("gateway-scale-out")
            second = "router2" if design == "baseline" else "gateway2"
            subprocess.run(command+["up", "-d", "--no-deps", second], check=True)
            observer.finish("gateway-scale-out")
            if design == "baseline":
                endpoints = ["grpc://"+subprocess.check_output(command+["port", s, "50050"], text=True).strip()
                             for s in ("router", "router2")]
            else:
                endpoints = [b.adr.endpoint(root, s) for s in ("gateway1", "gateway2")]
            run(design, root, observer, "two-gateways", endpoints=endpoints)
            observer.phase("gateway-scale-in")
            subprocess.run(command+["stop", "--timeout", "5", second], check=True)
            observer.finish("gateway-scale-in")
            run(design, root, observer, "one-gateway")
            event(design, "completed", {"phases": 11, "observer_samples": observer.samples})
        finally:
            try:
                if observer is not None:
                    observer.stop()
            finally:
                try:
                    with (evidence/(design+"-runtime.log")).open("w") as output:
                        subprocess.run(command+["logs", "--no-color"], stdout=output, stderr=subprocess.STDOUT, check=True)
                finally:
                    stop(command)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    main(parser.parse_args().run_id)
