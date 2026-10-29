"""Bounded native Flight adapter. Never monkey-patch or replay a DoPut."""

import base64
import hashlib
import hmac
import json
import struct
import time
import urllib.request
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.flight as flight

SIGNED_HEADERS = (
    "x-arrowcol-protocol-version", "x-arrowcol-implementation-pool",
    "x-arrowcol-catalog", "x-arrowcol-schema", "x-arrowcol-table",
    "x-arrowcol-schema-fingerprint", "x-arrowcol-control-operation", "content-type",
)
PUT_PATH = "/arrow.flight.protocol.FlightService/DoPut"


def signed_headers(secret, method, path, metadata=None):
    headers = dict(metadata or {})
    timestamp = str(time.time_ns() // 1_000_000)
    nonce = str(uuid.uuid4())
    canonical = f"{timestamp}\n{nonce}\n{method}\n{path}\n"
    canonical += "".join(f"{key}:{headers.get(key, '')}\n" for key in SIGNED_HEADERS)
    signature = base64.urlsafe_b64encode(
        hmac.new(secret, canonical.encode(), hashlib.sha256).digest()
    ).rstrip(b"=").decode()
    headers.update({
        "x-arrowcol-edge-timestamp": timestamp,
        "x-arrowcol-edge-nonce": nonce,
        "x-arrowcol-edge-signature": signature,
    })
    return headers


def load_sdk(root):
    root = Path(root)
    manifest = json.loads((root / "sdk-manifest.json").read_text())
    artifact = manifest["artifacts"]["arrow_schema"]
    raw = (root / artifact["path"]).read_bytes()
    if "sha256:" + hashlib.sha256(raw).hexdigest() != artifact["sha256"]:
        raise ValueError("canonical SDK schema digest mismatch")
    # SDK descriptor is a raw Arrow IPC Message, not a complete stream.
    framed = b"\xff\xff\xff\xff" + struct.pack("<I", len(raw)) + raw
    schema = pa.ipc.read_schema(pa.BufferReader(framed))
    metadata = manifest["flight"]["routing_metadata"]
    for key in ("catalog", "schema", "table"):
        if metadata[key] != manifest["table"][key]:
            raise ValueError("SDK routing identity mismatch")
    if metadata["schema_fingerprint"] != manifest["table"]["fingerprint"]:
        raise ValueError("SDK canonical fingerprint mismatch")
    return manifest, schema


def make_batch(schema, rows, marker):
    arrays = []
    for field in schema:
        kind = field.type
        if pa.types.is_string(kind) or pa.types.is_large_string(kind):
            values = [f"{marker}/{i}" if field.name == "sql_server_instance_name" else "synthetic" for i in range(rows)]
        elif pa.types.is_boolean(kind):
            values = [True] * rows
        elif pa.types.is_integer(kind):
            values = [1] * rows
        elif pa.types.is_floating(kind):
            values = [1.0] * rows
        elif pa.types.is_timestamp(kind):
            values = [1_790_000_000_000_000_000 // {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[kind.unit]] * rows
        else:
            raise ValueError(f"unsupported synthetic type: {kind}")
        arrays.append(pa.array(values, type=kind))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def upload(settings, sdk, ordinal):
    """One finite stream. A non-terminal disconnect has an unknown delivery outcome."""
    manifest, schema = sdk
    identity = manifest["table"]
    stream_id = f"{settings['run_id']}/{ordinal}"
    outcome = {
        "stream_id": stream_id, "table": ".".join(identity[k] for k in ("catalog", "schema", "table")),
        "generated_batches": 0, "attempted_batches": 0, "write_completed_batches": 0,
        "generated_rows": 0, "arrow_bytes": 0, "receipts": [],
        "terminal": "not_started", "markers": [], "setup_ms": None,
        "started_at_unix_ms": time.time_ns() // 1_000_000,
    }
    started = time.monotonic()
    client = None
    writer = None
    try:
        endpoint = settings["endpoints"][ordinal % len(settings["endpoints"])]
        outcome["endpoint"] = endpoint
        client = flight.FlightClient(endpoint, write_size_limit_bytes=settings["max_batch_bytes"])
        headers = {
            "content-type": "application/grpc",
            "x-arrowcol-protocol-version": "1",
            "x-arrowcol-implementation-pool": "direct",
            "x-arrowcol-catalog": identity["catalog"],
            "x-arrowcol-schema": identity["schema"],
            "x-arrowcol-table": identity["table"],
            "x-arrowcol-schema-fingerprint": identity["fingerprint"],
        }
        headers = signed_headers(settings["edge_secret"], "POST", PUT_PATH, headers)
        options = flight.FlightCallOptions(
            timeout=settings["timeout"],
            headers=[(k.encode(), v.encode()) for k, v in headers.items()],
        )
        descriptor = flight.FlightDescriptor.for_path(*manifest["flight"]["descriptor_path"])
        writer, reader = client.do_put(descriptor, schema, options=options)
        outcome["setup_ms"] = (time.monotonic() - started) * 1000
        outcome["terminal"] = "unknown"
        for batch_id in range(settings["batches_per_stream"]):
            if time.monotonic() - started >= settings["stream_seconds"]:
                break
            marker = f"{stream_id}/{batch_id}"
            batch = make_batch(schema, settings["rows"], marker)
            if batch.nbytes > settings["max_batch_bytes"]:
                raise ValueError("generated batch exceeds byte bound")
            outcome["generated_batches"] += 1
            outcome["generated_rows"] += batch.num_rows
            outcome["arrow_bytes"] += batch.nbytes
            outcome["markers"].extend(f"{marker}/{i}" for i in range(batch.num_rows))
            outcome["attempted_batches"] += 1
            writer.write_batch(batch)
            outcome["write_completed_batches"] += 1
            # Consume per-batch admission receipt: avoid filling the server's bounded result queue.
            while True:
                data = reader.read()
                if data is None:
                    raise RuntimeError("DoPut ended without an admission receipt")
                message = json.loads(data.to_pybytes())
                if message.get("kind") == "receipt":
                    if message.get("delivery_guarantee") != "durable_receipt":
                        raise RuntimeError("unexpected delivery guarantee")
                    if message.get("schema_fingerprint") != identity["fingerprint"] or message.get("wire_version") != 1:
                        raise RuntimeError("receipt schema fingerprint or wire version mismatch")
                    if message.get("summary", {}).get("rows") != batch.num_rows or message.get("summary", {}).get("batches") != 1:
                        raise RuntimeError("admitted batch summary mismatch")
                    outcome["receipts"].append({
                        "receipt_id": message["receipt_id"], "rows": batch.num_rows,
                        "marker_prefix": marker, "summary": message.get("summary"),
                        "schema_fingerprint": identity["fingerprint"],
                        "observed_at_unix_ms": time.time_ns() // 1_000_000,
                    })
                    break
                if message.get("kind") != "progress":
                    raise RuntimeError("unexpected DoPut result")
            if settings["pacing_ms"]:
                time.sleep(settings["pacing_ms"] / 1000)
        writer.done_writing()
        while reader.read() is not None:
            pass
        writer.close()
        writer = None
        outcome["terminal"] = "ok"
    except Exception as error:  # Native timeouts/errors do not authorize a replay.
        outcome["error"] = type(error).__name__ + ": " + str(error)[:512]
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                outcome["writer_cleanup_failed"] = True
        if client is not None:
            client.close()
        outcome["elapsed_ms"] = (time.monotonic() - started) * 1000
        outcome["finished_at_unix_ms"] = time.time_ns() // 1_000_000
    return outcome


def receipt_status(endpoint, secret, receipt_id, timeout=5):
    path = f"/v1/receipts/{receipt_id}"
    request = urllib.request.Request(
        endpoint.rstrip("/") + path, headers=signed_headers(secret, "GET", path)
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def visible_markers(delta_root, table):
    from deltalake import DeltaTable

    delta = DeltaTable(str(Path(delta_root).joinpath(*table.split("."))))
    dataset = delta.to_pyarrow_dataset()
    # One current fragment, no batch prefetch or threaded scan workers.
    data = dataset.to_table(
        columns=["sql_server_instance_name"], use_threads=False,
        batch_readahead=0, fragment_readahead=1,
    )
    markers = data.column(0).to_pylist()
    del data, dataset, delta
    return markers


def reconcile(settings, outcomes, delta_root):
    """Compare receipt-confirmed rows AND all visible run rows; retain unknown outcomes."""
    from collections import Counter

    deadline = time.monotonic() + settings["publication_timeout"]
    pending = {r["receipt_id"]: dict(r, table=o["table"]) for o in outcomes for r in o["receipts"]}
    published, failed = {}, {}
    while pending and time.monotonic() < deadline:
        for receipt_id, receipt in list(pending.items()):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                response = receipt_status(
                    settings["receipt_endpoint"], settings["edge_secret"], receipt_id, timeout=min(5, remaining)
                )
                status = response.get("status") or {}
                if status.get("state") == "published" and response.get("purge_allowed") is True:
                    publication = status.get("publication") or {}
                    # A Delta flush may publish several receipts in one commit. Its
                    # publication total is not the per-receipt admitted row count.
                    if publication.get("rows_published", 0) < receipt["rows"] or (
                        receipt.get("schema_fingerprint") is not None
                        and publication.get("schema_fingerprint") != receipt["schema_fingerprint"]
                    ):
                        failed[receipt_id] = dict(receipt, error="publication count or fingerprint mismatch")
                    else:
                        published[receipt_id] = dict(receipt, status=status)
                    del pending[receipt_id]
                elif status.get("state") in ("failed", "expired"):
                    failed[receipt_id] = dict(receipt, status=status)
                    del pending[receipt_id]
            except Exception as error:
                receipt["last_poll_error"] = type(error).__name__
        remaining = deadline - time.monotonic()
        if pending and remaining > 0:
            time.sleep(min(0.25, remaining))
    tables = sorted({o["table"] for o in outcomes})
    output = {}
    for table in tables:
        expected = Counter(
            f"{r['marker_prefix']}/{i}"
            for r in published.values() if r["table"] == table for i in range(r["rows"])
        )
        try:
            actual = Counter(v for v in visible_markers(delta_root, table) if v.startswith(settings["run_id"] + "/"))
            output[table] = {
                "receipt_published_rows": sum(expected.values()), "visible_run_rows": sum(actual.values()),
                "missing_rows": sum((expected - actual).values()), "unconfirmed_or_duplicate_rows": sum((actual - expected).values()),
            }
        except Exception as error:
            output[table] = {"error": type(error).__name__ + ": " + str(error)[:256]}
    return {
        "published": published, "failed": failed, "pending_or_unknown": pending, "output": output,
        "terminal_unknown_streams": sum(o["terminal"] != "ok" for o in outcomes),
        "reconciled": not pending and not failed and all(
            "error" not in v and v["missing_rows"] == 0 and v["unconfirmed_or_duplicate_rows"] == 0 for v in output.values()
        ),
        "exactly_once_claim": False,
    }
