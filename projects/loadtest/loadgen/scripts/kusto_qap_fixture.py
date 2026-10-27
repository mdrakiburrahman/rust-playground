"""Writes, mutates and VACUUMs the two-table Delta fixture used by kusto-qap-it.sh.

usage: kusto_qap_fixture.py ROOT init|mutate|vacuum
"""
import os
import sys
import datetime as dt
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

root, step = sys.argv[1], sys.argv[2]
base = f"{root}/telemetry/sqlserver"
SESS = f"{base}/sqlserver_active_sessions"
AG = f"{base}/sqlserver_availability_group_states"
ts = pa.timestamp("us", tz="UTC")


def sessions(start, n, batch):
    t0 = dt.datetime(2026, 10, 27, tzinfo=dt.timezone.utc)
    return pa.table({
        "customer_id": [f"c{i % 3}" for i in range(start, start + n)],
        "sample_time_utc": pa.array([t0 + dt.timedelta(microseconds=i * 1001) for i in range(start, start + n)], ts),
        "sql_server_instance_name": [f"inst{i % 2}" for i in range(start, start + n)],
        "machine_name": ["m'1\"x"] * n,
        "database_id": pa.array([i % 7 for i in range(start, start + n)], pa.int16()),
        "database_name": [f"db{i % 4}" for i in range(start, start + n)],
        "session_id": pa.array([i for i in range(start, start + n)], pa.int16()),
        "session_status": ["running"] * n,
        "connection_id": [f"conn-{i}" for i in range(start, start + n)],
        "__arrowcol_ingestion": [batch] * n,
    })


def ag(start, n, batch):
    t0 = dt.datetime(2026, 10, 27, tzinfo=dt.timezone.utc)
    return pa.table({
        "customer_id": [f"c{i % 3}" for i in range(start, start + n)],
        "sample_time_utc": pa.array([t0 + dt.timedelta(seconds=i) for i in range(start, start + n)], ts),
        "failure_condition_level": pa.array([i for i in range(start, start + n)], pa.int32()),
        "is_distributed": [i % 2 == 0 for i in range(start, start + n)],
        "lag_seconds": pa.array([i * 0.5 for i in range(start, start + n)], pa.float64()),
        "log_bytes": pa.array([i * 10_000_000_000 for i in range(start, start + n)], pa.int64()),
        "__arrowcol_ingestion": [batch] * n,
    })


if step == "init":
    for path, make in ((SESS, sessions), (AG, ag)):
        write_deltalake(path, make(0, 50, "b1"), mode="append", partition_by=["__arrowcol_ingestion"])
        write_deltalake(path, make(50, 50, "b2"), mode="append", partition_by=["__arrowcol_ingestion"])
elif step == "mutate":
    # dataChange=true partition PURGE on sessions, row delete + dataChange=false OPTIMIZE on AG.
    DeltaTable(SESS).delete("__arrowcol_ingestion = 'b1'")
    write_deltalake(SESS, sessions(100, 25, "b3"), mode="append", partition_by=["__arrowcol_ingestion"])
    DeltaTable(AG).delete("failure_condition_level < 10")
    DeltaTable(AG).optimize.compact()
elif step == "vacuum":
    for path in (SESS, AG):
        DeltaTable(path).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
for path in (SESS, AG):
    t = DeltaTable(path)
    print("TRUTH", step, path.rsplit("/", 1)[1], "v", t.version(), "files", len(t.file_uris()), "rows", t.to_pyarrow_table().num_rows)
sys.stdout.flush()
# deltalake can abort in native teardown after every commit is durable; skip interpreter teardown.
os._exit(0)
