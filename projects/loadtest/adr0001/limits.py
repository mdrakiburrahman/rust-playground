"""Finite live Flight admission/backpressure/cancellation probes, separate from benchmarks."""
import argparse
import json
from pathlib import Path
import pyarrow.flight as flight
import benchmark as b


def prove(root, baseline):
    root=Path(root)
    config=json.loads((root/"secrets/controller.json").read_text())
    manifest,schema=b.sdks(baseline)[0]
    key=b.table_key(manifest)
    def view():return b.adr.control(root,{"op":"view"})
    def record():return next(a["record"] for a in view()["assignments"] if a["record"]["key"]==key)
    def options():
        identity=manifest["table"]
        headers={"content-type":"application/grpc","x-arrowcol-protocol-version":"1",
                 "x-arrowcol-implementation-pool":"direct","x-arrowcol-catalog":identity["catalog"],
                 "x-arrowcol-schema":identity["schema"],"x-arrowcol-table":identity["table"],
                 "x-arrowcol-schema-fingerprint":identity["fingerprint"]}
        headers=b.native.signed_headers(config["edge_secret"].encode(),"POST",b.native.PUT_PATH,headers)
        return flight.FlightCallOptions(timeout=12,headers=[(k.encode(),v.encode()) for k,v in headers.items()])
    endpoint=b.adr.endpoint(root)
    descriptor=flight.FlightDescriptor.for_path(*manifest["flight"]["descriptor_path"])
    before=record()
    clients=[]
    handles=[]
    try:
        for _ in range(16):
            client=flight.FlightClient(endpoint);clients.append(client)
            writer,reader=client.do_put(descriptor,schema,options())
            # Retain both handles: schema-only streams hold slots without staging Arrow batches.
            handles.append((writer,reader))
        b.adr.wait(root,lambda v:sum(m["active"] for m in v["members"])>=16,seconds=8)
        excess=flight.FlightClient(endpoint)
        error=None
        try:
            writer,reader=excess.do_put(descriptor,schema,options())
            reader.read()
            writer.close()
        except Exception as failure:
            error=str(failure)
        finally:
            excess.close()
        assert error and ("capacity" in error or "exhausted" in error.lower()),error
    finally:
        for client in clients:client.close()
    b.adr.wait(root,lambda v:sum(m["active"] for m in v["members"])==0,seconds=15)
    assert record()["head"]==before["head"],"schema-only canceled streams published rows"
    oversized=flight.FlightClient(endpoint)
    large_error=None
    try:
        writer,reader=oversized.do_put(descriptor,schema,options())
        writer.write_batch(b.native.make_batch(schema,1,"large-"+("x"*(3*1024*1024))))
        writer.done_writing()
        reader.read()
        writer.close()
    except Exception as failure:
        large_error=str(failure)
    finally:
        oversized.close()
    assert large_error and any(s in large_error.lower() for s in ("larger","limit","maximum","exhausted")),large_error
    b.adr.wait(root,lambda v:sum(m["active"] for m in v["members"])==0,seconds=15)
    assert record()["head"]==before["head"],"rejected oversized unit changed head"
    result={"simultaneous_schema_streams":16,"excess_rejected":error,"canceled_streams_published":False,
            "oversized_rejected":large_error,"head_before":before["head"],"head_after":record()["head"]}
    (b.REPO/"target/adr0001/evidence/limits-proof.json").write_text(json.dumps(result,indent=2))


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("--root",required=True);p.add_argument("--baseline",required=True)
    a=p.parse_args();prove(a.root,a.baseline)
