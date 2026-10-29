"""Finite architecture benchmark phases. Fault experiments are separate from matched measurements."""
import argparse
import json
from pathlib import Path
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

import benchmark as b


def original(root, baseline):
    root = Path(root)
    stack = b.baseline_module(root)
    command = stack.context(root)[1]
    save = b.REPO / "target/adr0001/evidence/baseline-events.json"
    events = json.loads(save.read_text()) if save.exists() else []
    def event(kind, value):
        events.append({"kind": kind, "observed_ns": time.time_ns(), "value": value})
        save.write_text(json.dumps(events, indent=2))
    def run(label, **kwargs):
        existing = b.REPO / "target/adr0001/evidence/baseline" / label / "report.json"
        if existing.exists():
            result=json.loads(existing.read_text())
        else:
            result=b.run("baseline", root, baseline, label, **kwargs)
        assert result["reconciled"],f"baseline {label} did not reconcile"
        return result
    for n in range(1, 4):
        run(f"warm-{n:02}")
    stress = subprocess.Popen(command + ["exec", "-T", "direct-1", "timeout", "18", "sh", "-c",
        "while :; do :; done"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    event("pressure", b.resource_snapshot(command))
    run("pressure", indexes=[3, 4, 5], streams=12)
    stress.wait(timeout=22)
    run("elephant", indexes=[0], streams=18, rows=64)
    view = stack.control(root, "view")["value"]
    lease = next(l for l in view["leases"] if l["state"] == "Active")
    event("before-migration", lease)
    source = next(c for c in view["collectors"] if c["registration"]["instance_id"] == lease["holder_instance_id"])
    service = "direct-" + source["registration"]["instance_key"].rsplit("-", 1)[1]
    pressure = subprocess.Popen(command + ["exec", "-T", service, "timeout", "20", "sh", "-c",
        "while :; do :; done"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        current = stack.control(root, "view")["value"]
        measured = next(c for c in current["collectors"] if c["registration"]["instance_id"] == lease["holder_instance_id"])
        if measured["pressure"]["cpu_utilization_bps"] > 2500:
            event("migration-pressure", current)
            break
        time.sleep(0.25)
    event("migration", stack.move(root, lease["key"]))
    pressure.wait(timeout=25)
    run("after-move")
    subprocess.run(command + ["up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "direct-4"], check=True)
    time.sleep(3)
    event("four-collectors", stack.control(root, "view"))
    run("four-new", indexes=[9, 10, 11], streams=12)
    event("scale-in", stack.scale_in(root))
    run("after-scale-in", indexes=[9, 10, 11], streams=12)
    subprocess.run(command + ["up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "router2"], check=True)
    endpoints = ["grpc://" + subprocess.check_output(command + ["port", service, "50050"], text=True).strip()
                 for service in ("router", "router2")]
    run("two-gateways", endpoints=endpoints)
    event("two-gateways", stack.control(root, "view"))
    subprocess.run(command + ["stop", "--timeout", "95", "router2"], check=True)
    run("one-gateway")
    event("unsupported", ["atomic etcd/head fault cases", "no-WAL replay contract", "automatic collector-death recovery"])


def new(root, baseline):
    root = Path(root)
    command = b.adr.compose(root)
    events = []
    save = b.REPO / "target/adr0001/evidence/adr-events.json"
    def event(kind, value):
        events.append({"kind": kind, "observed_ns": time.time_ns(), "value": value})
        save.write_text(json.dumps(events, indent=2))
    def view():
        return b.adr.control(root, {"op": "view"})
    def run(label, **kwargs):
        result=b.run("adr", root, baseline, label, **kwargs)
        assert result["reconciled"],f"adr {label} did not reconcile"
        return result
    def move(key):
        before = view()
        old = next(a["record"] for a in before["assignments"] if a["record"]["key"] == key)
        start = time.monotonic()
        operation = b.adr.control(root, {"op": "migrate", "key": key})
        after = b.adr.wait(root, lambda v: any(a["record"]["key"] == key and a["record"]["epoch"] > old["epoch"]
            and a["record"]["lifecycle"] == "Active" for a in v["assignments"]))
        record = next(a["record"] for a in after["assignments"] if a["record"]["key"] == key)
        event("migration", {"operation": operation, "after": record, "seconds": time.monotonic()-start})
        return record
    run("cold-02")
    for n in range(1, 4):
        run(f"warm-{n:02}")
    stress = subprocess.Popen(command + ["exec", "-T", "collector1", "timeout", "18", "sh", "-c",
        "while :; do :; done"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    event("pressure", b.resource_snapshot(command))
    run("pressure", indexes=[3, 4, 5], streams=12)
    stress.wait(timeout=22)
    run("elephant", indexes=[0], streams=18, rows=64)
    move(view()["assignments"][0]["record"]["key"])
    run("after-move")
    subprocess.run(command + ["up", "-d", "--no-deps", "collector4"], check=True)
    b.adr.wait(root, lambda v: len(v["members"]) == 4)
    event("four-collectors", view())
    run("four-new", indexes=[9, 10, 11], streams=12)
    fourth = [a["record"]["key"] for a in view()["assignments"] if a["record"]["writer"]["collector"] == "collector4"]
    if not fourth:
        raise RuntimeError("fourth collector received no new table")
    inbound_key=next(a["record"]["key"] for a in view()["assignments"]
                     if a["record"]["writer"]["collector"]!="collector4")
    pressures=[subprocess.Popen(command+["exec","-T",f"collector{i}","timeout","18","sh","-c",
        "while :; do :; done"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL) for i in range(1,4)]
    try:
        measured=b.adr.wait(root,lambda v: all(m["pressure"]>.5 for m in v["members"]
            if m["writer"]["collector"]!="collector4"),seconds=12)
        event("inbound-pressure",measured)
        inbound=move(inbound_key)
        assert inbound["writer"]["collector"]=="collector4","controller did not choose unpressured added capacity"
        event("inbound-fourth",inbound)
    finally:
        for process in pressures:process.wait(timeout=25)
    fourth=[a["record"]["key"] for a in view()["assignments"] if a["record"]["writer"]["collector"]=="collector4"]
    b.adr.control(root, {"op": "drain_collector", "collector": "collector4"})
    b.adr.wait(root, lambda v: any(m["writer"]["collector"] == "collector4" and not m["eligible"] for m in v["members"]))
    for key in fourth:
        move(key)
    b.adr.wait(root, lambda v: all(a["record"]["writer"]["collector"] != "collector4"
        and a["record"]["lifecycle"] == "Active" for a in v["assignments"]))
    event("scale-in", {"evacuated_tables": fourth, "view": view(), "mode": "drain, fenced cutover, recovery and activation before removal"})
    subprocess.run(command + ["stop", "--timeout", "5", "collector4"], check=True)
    b.adr.control(root, {"op": "revoke", "collector": "collector4"})
    run("after-scale-in", indexes=[9, 10, 11], streams=12)
    subprocess.run(command + ["up", "-d", "--no-deps", "gateway2"], check=True)
    run("two-gateways", endpoints=[b.adr.endpoint(root), b.adr.endpoint(root, "gateway2")])
    event("two-gateways", view())
    subprocess.run(command + ["stop", "--timeout", "5", "gateway2"], check=True)
    run("one-gateway")
    run("lost-reply", indexes=[0], streams=3, max_attempts=3, lose_first_reply=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("design", choices=["baseline", "adr"])
    parser.add_argument("--root", required=True)
    parser.add_argument("--baseline", required=True)
    args = parser.parse_args()
    (original if args.design == "baseline" else new)(args.root, args.baseline)
