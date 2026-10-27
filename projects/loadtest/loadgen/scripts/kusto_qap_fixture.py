"""Writes, mutates and VACUUMs the Delta fixtures used by kusto-qap-it.sh.

usage: kusto_qap_fixture.py ROOT init|mutate|vacuum
       kusto_qap_fixture.py ROOT schemas SQLSERVER_SQL

`schemas` writes all canonical tables from sqlserver.sql in the collectors' normalized Delta
layout (microsecond timestamp followed by its `__unix_nanos` companion, partitioned by
`__arrowcol_ingestion`) with boundary values for every declared type.
"""
import os
import re
import sys
import datetime as dt
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

root, step = sys.argv[1], sys.argv[2]
base = f"{root}/telemetry/sqlserver"
SESS = f"{base}/sqlserver_active_sessions"
AG = f"{base}/sqlserver_availability_group_states"
ts = pa.timestamp("us", tz="UTC")
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
TEXT = ["", "plain", "quote ' and \" mix", "back\\slash", "tab\tnew\nline", "unicodé ✓ 漢字", "x" * 300]
BOUNDS = {
    "TINYINT": (pa.int8(), [-128, 127, 0, -1, 1]),
    # Collectors normalize UInt8 to Int16 because Delta has no unsigned types.
    "TINYINT UNSIGNED": (pa.int16(), [0, 255, 1, 128, 254]),
    "SMALLINT": (pa.int16(), [-32768, 32767, 0, -1, 1]),
    "INT": (pa.int32(), [-(2**31), 2**31 - 1, 0, -1, 1]),
    "BIGINT": (pa.int64(), [-(2**63), 2**63 - 1, 0, -1, 1]),
    "DOUBLE": (pa.float64(), [0.0, -0.5, 1.0e308, -2.2250738585072014e-308, 123456.789012345]),
    "BOOLEAN": (pa.bool_(), [True, False]),
}
# Pre-epoch, sub-microsecond, and far-future instants exercise the precision companion.
NANOS = [-1, 0, 999, 1_793_073_575_123_456_789, 4_102_444_799_999_999_999, -86_400_000_000_001]


def canonical_tables(sql):
    for name, body in re.findall(r"CREATE TABLE telemetry\.sqlserver\.(\w+) \((.*?)\);", sql, re.S):
        yield name, [re.match(r"\s*(\w+) (.+?) NOT NULL", line).groups() for line in body.strip().split(",\n")]


def canonical_batch(columns, start, n, batch):
    data, fields = {}, []
    for index, (name, kind) in enumerate(columns):
        rows = range(start, start + n)
        if kind.startswith("TIMESTAMP(9)"):
            nanos = [NANOS[(i + index) % len(NANOS)] + i for i in rows]
            micros = [value // 1000 for value in nanos]
            data[name] = pa.array([EPOCH + dt.timedelta(microseconds=m) for m in micros], ts)
            data[f"{name}__unix_nanos"] = pa.array(nanos, pa.int64())
            fields += [pa.field(name, ts, False), pa.field(f"{name}__unix_nanos", pa.int64(), False)]
            continue
        if kind == "VARCHAR":
            arrow, values = pa.string(), [f"{TEXT[(i + index) % len(TEXT)]}#{i}" if i % 5 else TEXT[(i + index) % len(TEXT)] for i in rows]
        else:
            arrow, choices = BOUNDS[kind]
            values = [choices[(i + index) % len(choices)] for i in rows]
        data[name] = pa.array(values, arrow)
        fields.append(pa.field(name, arrow, False))
    data["__arrowcol_ingestion"] = pa.array([batch] * n, pa.string())
    fields.append(pa.field("__arrowcol_ingestion", pa.string(), False))
    return pa.table(data, schema=pa.schema(fields))


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
paths = (SESS, AG)
if step == "schemas":
    with open(sys.argv[3], encoding="utf-8") as source:
        tables = list(canonical_tables(source.read()))
    assert len(tables) == 13, f"expected 13 canonical tables, found {len(tables)}"
    paths = []
    for name, columns in tables:
        path = f"{base}/{name}"
        batch = canonical_batch(columns, 0, 60, "b1")
        for (column, kind) in columns:
            values = batch.column(f"{column}__unix_nanos" if kind.startswith("TIMESTAMP(9)") else column).to_pylist()
            if kind.startswith("TIMESTAMP(9)"):
                assert any(value % 1000 for value in values), f"{name}.{column} lacks sub-microsecond nanos"
                assert any(value < 0 for value in values), f"{name}.{column} lacks pre-epoch nanos"
            elif kind == "TINYINT UNSIGNED":
                assert {128, 255} <= set(values), f"{name}.{column} lacks unsigned edges"
        write_deltalake(path, batch.slice(0, 30), mode="append", partition_by=["__arrowcol_ingestion"])
        write_deltalake(path, canonical_batch(columns, 30, 30, "b2"), mode="append", partition_by=["__arrowcol_ingestion"])
        paths.append(path)
for path in paths:
    t = DeltaTable(path)
    print("TRUTH", step, path.rsplit("/", 1)[1], "v", t.version(), "files", len(t.file_uris()), "rows", t.to_pyarrow_table().num_rows)
sys.stdout.flush()
# deltalake can abort in native teardown after every commit is durable; skip interpreter teardown.
os._exit(0)
