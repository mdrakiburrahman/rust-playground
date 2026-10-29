import hashlib
import json
import shutil
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import gevent
import pyarrow as pa
import pyarrow.flight as flight
from gevent.threadpool import ThreadPool

from native import load_sdk, reconcile, signed_headers, upload
from stack import REPO, context


class NativeServer(flight.FlightServerBase):
    def __init__(self):
        super().__init__("grpc://127.0.0.1:0")
        self.release = threading.Event()
        self.calls = 0
        self.block_first = False
        self.schema = pa.schema([pa.field("sql_server_instance_name", pa.string(), nullable=False)])

    def do_put(self, context, descriptor, reader, writer):
        self.calls += 1
        if reader.schema != self.schema:
            raise pa.ArrowInvalid("invalid schema")
        for chunk in reader:
            marker = chunk.data.column(0)[0].as_py()
            if self.block_first and marker.split("/")[1] == "0":
                self.release.wait(3)
            writer.write(pa.py_buffer(json.dumps({
                "kind": "receipt", "receipt_id": str(uuid.uuid4()), "delivery_guarantee": "durable_receipt",
                "wire_version": 1, "schema_fingerprint": "sha256:" + "0" * 64,
                "summary": {"rows": chunk.data.num_rows, "batches": 1},
            }).encode()))


class NativeTests(unittest.TestCase):
    def setUp(self):
        self.root = REPO / "target/native-benchmark-tests" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.server = NativeServer()
        self.settings = {
            "run_id": "native-unit", "endpoints": [f"grpc://127.0.0.1:{self.server.port}"],
            "rows": 2, "batches_per_stream": 2, "stream_seconds": 1,
            "timeout": 2, "max_batch_bytes": 1_048_576, "pacing_ms": 0,
            "edge_secret": b"x" * 32, "publication_timeout": 1, "receipt_endpoint": "http://unused",
        }
        self.manifest = {
            "table": {"catalog": "telemetry", "schema": "sqlserver", "table": "events", "fingerprint": "sha256:" + "0" * 64},
            "flight": {"descriptor_path": ["telemetry", "sqlserver", "events", "v1"]},
        }

    def tearDown(self):
        self.server.release.set()
        self.server.shutdown()
        shutil.rmtree(self.root)

    def test_native_blocked_stream_does_not_serialize_other_locust_workers(self):
        self.server.block_first = True
        pool = ThreadPool(2)
        try:
            first = pool.spawn(upload, self.settings, (self.manifest, self.server.schema), 0)
            gevent.sleep(0.1)
            second = pool.spawn(upload, self.settings, (self.manifest, self.server.schema), 1)
            outcome = second.get(timeout=2)
            self.assertEqual(outcome["terminal"], "ok", outcome)
            self.assertFalse(first.ready(), "unrelated stream progressed while the first native call blocked")
            self.server.release.set()
            self.assertEqual(first.get(timeout=2)["terminal"], "ok")
        finally:
            self.server.release.set()
            with gevent.Timeout(4, False):
                pool.join()
            pool.kill()

    def test_native_rpc_timeout_is_unknown_and_never_replayed(self):
        self.server.block_first = True
        settings = dict(self.settings, timeout=1)
        started = time.monotonic()
        outcome = upload(settings, (self.manifest, self.server.schema), 0)
        self.assertNotEqual(outcome["terminal"], "ok")
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(self.server.calls, 1)

    def test_bad_schema_fails_without_receipts_or_replay(self):
        wrong_schema = pa.schema([pa.field("wrong", pa.int64(), nullable=False)])
        outcome = upload(self.settings, (self.manifest, wrong_schema), 1)
        self.assertNotEqual(outcome["terminal"], "ok")
        self.assertEqual(outcome["receipts"], [])
        self.assertEqual(self.server.calls, 1)

    def test_canonical_sdk_descriptor_hash_and_metadata_are_validated(self):
        raw = self.server.schema.serialize().to_pybytes()[8:]
        path = self.root / "descriptors/schema.arrow"
        path.parent.mkdir()
        path.write_bytes(raw)
        manifest = dict(self.manifest)
        manifest["artifacts"] = {"arrow_schema": {"path": "descriptors/schema.arrow", "sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}}
        manifest["flight"]["routing_metadata"] = {
            **manifest["table"], "schema_fingerprint": manifest["table"]["fingerprint"],
        }
        (self.root / "sdk-manifest.json").write_text(json.dumps(manifest))
        self.assertEqual(load_sdk(self.root)[1], self.server.schema)
        path.write_bytes(raw + b"bad")
        with self.assertRaises(ValueError):
            load_sdk(self.root)

    def test_publication_requires_purge_permission_and_independent_output(self):
        from deltalake import write_deltalake

        table = "telemetry.sqlserver.events"
        write_deltalake(str(self.root / "telemetry/sqlserver/events"), pa.table({
            "sql_server_instance_name": ["native-unit/0/0/0", "native-unit/0/0/1"],
        }))
        outcomes = [{"table": table, "terminal": "ok", "receipts": [{
            "receipt_id": "receipt", "rows": 2, "marker_prefix": "native-unit/0/0",
        }]}]
        published = {"status": {"state": "published", "publication": {"rows_published": 2}}, "purge_allowed": True}
        with patch("native.receipt_status", return_value=published):
            self.assertTrue(reconcile(self.settings, outcomes, self.root)["reconciled"])
        with patch("native.receipt_status", return_value=dict(published, purge_allowed=False)):
            report = reconcile(self.settings, outcomes, self.root)
            self.assertFalse(report["reconciled"])
            self.assertEqual(len(report["pending_or_unknown"]), 1)
        published["status"]["publication"]["rows_published"] = 1
        with patch("native.receipt_status", return_value=published):
            self.assertFalse(reconcile(self.settings, outcomes, self.root)["reconciled"])

    def test_signer_binds_method_path_metadata_and_content_type(self):
        with patch("native.time.time_ns", return_value=1_000_000_000), patch("native.uuid.uuid4", return_value="fixed"):
            first = signed_headers(b"x" * 32, "POST", "/DoPut", {"content-type": "application/grpc"})
            changed = signed_headers(b"x" * 32, "POST", "/Other", {"content-type": "application/grpc"})
            self.assertNotEqual(first["x-arrowcol-edge-signature"], changed["x-arrowcol-edge-signature"])

    def test_publication_deadline_bounds_the_whole_receipt_set_not_each_request(self):
        clock = [0.0]
        outcomes = [{"table": "telemetry.sqlserver.events", "terminal": "ok", "receipts": [
            {"receipt_id": str(i), "rows": 1, "marker_prefix": f"native-unit/0/{i}"} for i in range(3)
        ]}]

        def unavailable(endpoint, secret, receipt_id, timeout=5):
            clock[0] += timeout
            raise TimeoutError("receipt endpoint unavailable")

        with patch("native.time.monotonic", side_effect=lambda: clock[0]), \
                patch("native.receipt_status", side_effect=unavailable) as status:
            report = reconcile(self.settings, outcomes, self.root)
        self.assertEqual(status.call_count, 1)
        self.assertEqual(clock[0], self.settings["publication_timeout"])
        self.assertEqual(len(report["pending_or_unknown"]), 3)
        self.assertFalse(report["reconciled"])

    def test_lifecycle_refuses_unowned_directory(self):
        with self.assertRaises(FileNotFoundError):
            context(self.root)

    def test_scale_in_refuses_stale_or_nonzero_pressure_without_stopping_source(self):
        import stack
        now = time.time_ns() // 1_000_000
        pressure = {name: {"current": 0} for name in
                    ("active_streams", "buffered_bytes", "buffered_rows", "buffered_batches", "pending_commits")}
        pressure["observed_at_unix_ms"] = now
        collector = {"registration": {"instance_key": "routing-benchmark-direct-4", "instance_id": "four"},
                     "pressure": pressure}
        view = {"result": "view", "value": {"collectors": [collector], "leases": []}}
        for stale, busy in ((False, True), (True, False)):
            pressure["observed_at_unix_ms"] = now - (60_000 if stale else 0)
            pressure["active_streams"]["current"] = int(busy)
            with patch("stack.context", return_value=({"project": "pingora-bench-fixture"}, ["scoped-compose"])), \
                    patch("stack.control", return_value=view), patch("stack.subprocess.run") as stop:
                with self.assertRaises(RuntimeError):
                    stack.scale_in(self.root)
                stop.assert_not_called()

    def test_generated_topology_preserves_state_and_separates_controller_from_data_router(self):
        import stack
        import yaml
        project = "pingora-bench-fixture-" + uuid.uuid4().hex
        root = stack.REPO / "target/routing-benchmark" / project
        try:
            stack.prepare(project, "10.198.38.0/24", "least_pressure")
            token = (root / "secrets/operator_token").read_bytes()
            snapshot = root / "controller-state/routing/routing.json"
            snapshot.parent.mkdir()
            snapshot.write_bytes(b"existing-authority-not-a-health-cache")
            stack.prepare(project, "10.198.38.0/24", "least_pressure", resume=True)
            self.assertEqual((root / "secrets/operator_token").read_bytes(), token)
            self.assertEqual(snapshot.read_bytes(), b"existing-authority-not-a-health-cache")
            compose = yaml.safe_load((root / "compose.yaml").read_text())
            controller, data = [compose["services"][name] for name in ("router", "router2")]
            self.assertEqual(controller["environment"]["ARROWCOL_ROUTING_MODE"], "controller")
            self.assertEqual(data["environment"]["ARROWCOL_ROUTING_MODE"], "data_only")
            self.assertNotIn("ARROWCOL_ROUTING_DIRECTORY", data["environment"])
            self.assertNotIn("ARROWCOL_CONTROL_CREDENTIALS_FILE", data["environment"])
            self.assertTrue(all(port.startswith("127.0.0.1:") for port in controller["ports"] + data["ports"]))
            self.assertNotIn("50060", " ".join(controller["ports"]))
            for ordinal in range(1, 5):
                self.assertNotIn("ports", compose["services"][f"direct-{ordinal}"])
            self.assertEqual(compose["services"]["direct-4"]["profiles"], ["scale"])
            self.assertEqual(data["profiles"], ["scale"])
        finally:
            if root.exists():
                shutil.rmtree(root)


if __name__ == "__main__":
    unittest.main()
