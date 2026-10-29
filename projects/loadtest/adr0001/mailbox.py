"""Concurrent one-table writes while a second process attempts the same collector identity."""
import argparse
import json
from pathlib import Path
import subprocess
import benchmark as b


def prove(root, baseline):
    root=Path(root)
    key=b.table_key(b.sdks(baseline)[0][0])
    def record():
        return next(a["record"] for a in b.adr.control(root,{"op":"view"})["assignments"]
                    if a["record"]["key"]==key)
    before=record()
    role=before["writer"]["collector"]
    log=b.REPO/"target/adr0001/evidence/overlap-process.log"
    with log.open("w") as output:
        process=subprocess.Popen(b.adr.compose(root)+["run","--rm","--no-deps","--name",
            root.name+"-overlap","--entrypoint","timeout",role,"8","/native/adr-flight",
            "--config","/config.json","--collector",role],stdout=output,stderr=subprocess.STDOUT)
        report=b.run("adr",root,baseline,"writer-overlap",indexes=[0],streams=18)
        code=process.wait(timeout=20)
    after=record()
    assert code==124,code
    assert report["reconciled"],report
    assert before["writer"]==after["writer"],"overlapping incarnation replaced live owner"
    assert after["head"]["sequence"]-before["head"]["sequence"]==36,"lost/duplicate head transition"
    assert "session cannot silently change generation" in log.read_text() or "old process session still live" in log.read_text()
    result={"before":before,"after":after,"streams":18,"batches":36,
            "duplicate_process_exit":code,"authoritative_rows_reconciled":report["visible_rows"],
            "unchanged_live_generation":True}
    (b.REPO/"target/adr0001/evidence/mailbox-proof.json").write_text(json.dumps(result,indent=2))


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",required=True);p.add_argument("--baseline",required=True)
    a=p.parse_args();prove(a.root,a.baseline)
