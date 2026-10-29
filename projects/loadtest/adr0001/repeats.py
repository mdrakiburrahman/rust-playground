"""Two additional cold replicates per design, each with an independent scoped dataset."""
import json
from pathlib import Path
import subprocess
import time
import benchmark as b


def main():
    evidence=b.REPO/"target/adr0001/evidence"
    first=Path((evidence/"fresh-baseline-root.txt").read_text().strip())
    original=b.baseline_module(first)
    events=[]
    def save(kind,value):
        events.append({"kind":kind,"observed_ns":time.time_ns(),"value":value})
        (evidence/"cold-replicates.json").write_text(json.dumps(events,indent=2))
    def stop(command):
        subprocess.run(command+["--profile","*","stop","--timeout","4"],check=True)
    def logs(command,name):
        with (evidence/name).open("w") as output:
            subprocess.run(command+["logs","--no-color"],stdout=output,stderr=subprocess.STDOUT,check=True)
    # Main measured stack must be stopped before any other architecture runs.
    matched=b.REPO/"target/adr0001/adr0001-matched-20261026"
    logs(b.adr.compose(matched),"matched-runtime.log")
    stop(b.adr.compose(matched))
    for ordinal in (2,3):
        label=f"cold-repeat-{ordinal:02}"
        project=f"pingora-bench-adrcold{ordinal}-20261026"
        root=original.prepare(project,f"10.198.{76+ordinal}.0/24","least_pressure")
        command=original.context(root)[1]
        for image in ("runtime","sqlserver","hive"):
            subprocess.run(["docker","--context","default","tag",f"{first.name}/{image}:local",
                            f"{project}/{image}:local"],check=True)
        try:
            subprocess.run(command+["up","-d","--no-build","--wait","--wait-timeout","120",
                "router","direct-1","direct-2","direct-3"],check=True)
            subprocess.run(command+["run","--rm","--no-deps","prepare"],check=True)
            result=b.run("baseline",root,root,label)
            assert result["reconciled"],label
            save("cold-baseline",{"root":str(root),"label":label,"report":result["reconciled"]})
            idle=[]
            for _ in range(2):
                idle.append({"observed_ns":time.time_ns(),"resources":b.resource_snapshot(command)})
                time.sleep(1)
            save("idle-baseline",{"root":str(root),"samples":idle})
            if ordinal==2:
                subprocess.run(command+["up","-d","--no-deps","--wait","direct-4"],check=True)
                stress=[subprocess.Popen(command+["exec","-T",f"direct-{i}","timeout","20","sh","-c",
                    "while :; do :; done"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL) for i in range(1,4)]
                try:
                    deadline=time.monotonic()+14
                    while time.monotonic()<deadline:
                        view=original.control(root,"view")["value"]
                        busy=[c for c in view["collectors"] if c["registration"]["instance_key"]!="routing-benchmark-direct-4"]
                        if len(busy)==3 and all(c["pressure"]["cpu_utilization_bps"]>2500 for c in busy):
                            break
                        time.sleep(.25)
                    else:
                        raise AssertionError("actual original collector pressure did not converge")
                    target=next(c for c in view["collectors"] if c["registration"]["instance_key"]=="routing-benchmark-direct-4")
                    move=original.move(root,view["leases"][0]["key"])
                    assert move["lease"]["holder_instance_id"]==target["registration"]["instance_id"]
                    save("baseline-inbound-fourth",{"pressure":view,"migration":move})
                finally:
                    for process in stress:process.wait(timeout=25)
                save("baseline-evacuate-fourth",original.scale_in(root))
            logs(command,f"cold-baseline-{ordinal}.log")
        finally:
            stop(command)
        adrroot=b.adr.prepare(f"adr0001-cold{ordinal}-20261026",b.catalog(root))
        command=b.adr.compose(adrroot)
        try:
            b.adr.start(adrroot)
            subprocess.run(command+["up","-d","--no-deps","collector3"],check=True)
            b.adr.wait(adrroot,lambda v:len(v["members"])==3)
            result=b.run("adr",adrroot,root,label)
            assert result["reconciled"],label
            save("cold-adr",{"root":str(adrroot),"label":label,"report":result["reconciled"]})
            idle=[]
            for _ in range(2):
                idle.append({"observed_ns":time.time_ns(),"resources":b.resource_snapshot(command)})
                time.sleep(1)
            save("idle-adr",{"root":str(adrroot),"samples":idle})
            logs(command,f"cold-adr-{ordinal}.log")
        finally:
            stop(command)


if __name__=="__main__":
    main()
