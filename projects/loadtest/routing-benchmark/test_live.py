"""Opt-in admission checks against the real, already-running task-owned stack."""

import json
import os
import time
import unittest
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.flight as flight

from experiment import Experiment
from native import PUT_PATH, make_batch, signed_headers, upload
from stack import control, move
from workload import save_report, settings_from_file


@unittest.skipUnless(os.environ.get("ARROWCOL_BENCHMARK_LIVE_ROOT"), "requires a prepared live benchmark stack")
class LiveAdmissionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.experiment = Experiment(os.environ["ARROWCOL_BENCHMARK_LIVE_ROOT"], "live-admission", repetitions=1)
        path = cls.experiment.settings(
            "valid-flight", [cls.experiment.tables[0]["identity"]],
            workers=1, streams=1, rows=2, batches_per_stream=1, timeout=5, stream_seconds=2,
        )
        cls.settings, sdks = settings_from_file(path)
        cls.sdk = sdks[0]

    @classmethod
    def tearDownClass(cls):
        cls.experiment.finish()

    def routing_failures(self, operation, reason):
        with urllib.request.urlopen(self.settings["receipt_endpoint"] + "/metrics", timeout=5) as response:
            metrics = response.read().decode()
        return sum(float(line.rsplit(" ", 1)[1]) for line in metrics.splitlines()
                   if line.startswith("arrowcol_router_failures_total{")
                   and f'operation="{operation}"' in line and f'reason="{reason}"' in line)

    def rejected(self, name, *, wrong_secret=False, protocol="1", wrong_schema=False):
        manifest, schema = self.sdk
        identity = manifest["table"]
        headers = {
            "content-type": "application/grpc", "x-arrowcol-protocol-version": protocol,
            "x-arrowcol-implementation-pool": "direct",
            **{"x-arrowcol-" + field: identity[field] for field in ("catalog", "schema", "table")},
            "x-arrowcol-schema-fingerprint": identity["fingerprint"],
        }
        secret = b"not-the-task-edge-secret-" * 2 if wrong_secret else self.settings["edge_secret"]
        headers = signed_headers(secret, "POST", PUT_PATH, headers)
        options = flight.FlightCallOptions(
            timeout=5, headers=[(key.encode(), value.encode()) for key, value in headers.items()]
        )
        if wrong_schema:
            schema = pa.schema([pa.field("unexpected_field", pa.int64(), nullable=False)])
        client = flight.FlightClient(self.settings["endpoints"][0])
        writer = None
        receipts = []
        failure_labels = ("auth", "invalid") if wrong_secret else ("doput", "invalid_metadata") if protocol != "1" else None
        before = self.routing_failures(*failure_labels) if failure_labels else None
        try:
            with self.assertRaises((flight.FlightError, pa.ArrowInvalid)) as rejected:
                writer, reader = client.do_put(
                    flight.FlightDescriptor.for_path(*manifest["flight"]["descriptor_path"]), schema, options
                )
                writer.write_batch(make_batch(schema, 1, "rejected-admission/0/0"))
                writer.done_writing()
                while data := reader.read():
                    receipts.append(json.loads(data.to_pybytes()))
                writer.close()
            message = str(rejected.exception)
            self.assertFalse(any(result.get("kind") == "receipt" for result in receipts))
            after = self.routing_failures(*failure_labels) if failure_labels else None
            if failure_labels:
                self.assertEqual(after - before, 1)
            (self.experiment.output / (name + ".json")).write_text(json.dumps({
                "rejected": True, "error": message, "admission_receipts": receipts, "replayed": False,
                "router_failure_labels": failure_labels,
                "router_failure_count_before": before, "router_failure_count_after": after,
            }, indent=2))
            return message
        finally:
            if writer is not None:
                try:
                    writer.close()
                except (flight.FlightError, pa.ArrowInvalid):
                    pass
            client.close()

    def test_bad_edge_hmac_is_rejected_before_admission(self):
        self.rejected("bad-edge-hmac", wrong_secret=True)

    def test_unsupported_protocol_metadata_is_rejected(self):
        self.rejected("unsupported-protocol", protocol="999")

    def test_claimed_canonical_fingerprint_cannot_admit_wrong_wire_schema(self):
        self.assertRegex(self.rejected("wrong-wire-schema", wrong_schema=True).lower(), "schema|fingerprint")

    def test_draining_rejects_new_rpc_and_bounds_observer_without_discarding_admitted_work(self):
        table = self.sdk[0]["table"]
        key = {"implementation_pool": "direct", "table": {k: table[k] for k in ("catalog", "schema", "table")}}
        identity = ".".join(key["table"][k] for k in ("catalog", "schema", "table"))
        source_settings, _ = settings_from_file(self.experiment.settings(
            "draining-source", [identity], workers=1, streams=1,
            batches_per_stream=48, pacing_ms=250, stream_seconds=20,
        ))
        rejected_settings, _ = settings_from_file(self.experiment.settings(
            "draining-rejected", [identity], workers=1, streams=1,
            timeout=5, stream_seconds=2, batches_per_stream=1,
        ))
        before = control(self.experiment.root, "view")["value"]
        lease = next(row for row in before["leases"] if row["key"] == key)
        collector = next(row for row in before["collectors"]
                         if row["registration"]["instance_id"] == lease["holder_instance_id"])
        service = "direct-" + collector["registration"]["instance_key"].rsplit("-", 1)[1]
        started = time.monotonic()
        source = None
        try:
            with ThreadPoolExecutor(max_workers=1) as workers:
                source = workers.submit(upload, source_settings, self.sdk, 0)
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    view = control(self.experiment.root, "view")["value"]
                    owner = next(row for row in view["collectors"]
                                 if row["registration"]["instance_id"] == lease["holder_instance_id"])
                    if (owner.get("pressure") or {}).get("active_streams", {}).get("current", 0) > 0:
                        break
                    self.assertFalse(source.done(), "source ended before admission observation")
                    time.sleep(0.2)
                else:
                    self.fail("source admission was not observed")
                self.experiment.pressure(service)
                time.sleep(2)
                record = control(self.experiment.root, "migrate", [key, "bounded drain observation", False])
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    view = control(self.experiment.root, "view")["value"]
                    current = next(row for row in view["leases"] if row["key"] == key)
                    if current["state"] == "Draining":
                        break
                    time.sleep(0.2)
                else:
                    self.fail("source draining barrier was not observed")
                with self.assertRaises(TimeoutError):
                    move(self.experiment.root, key, timeout=0.2, record=record)
                (self.experiment.output / "bounded-drain-blocked.json").write_text(json.dumps({
                    "observer_timed_out": True, "source_still_admitted": not source.done(),
                    "force_removed_or_reassigned": False, "draining_lease": current,
                }, indent=2))
                rejected_at = time.monotonic()
                failures_before = self.routing_failures("doput", "lease_draining")
                rejected = upload(rejected_settings, self.sdk, 0)
                report = save_report(rejected_settings, [rejected], rejected_at)
                self.assertFalse(report["successful_pilot"])
                self.assertNotEqual(rejected["terminal"], "ok")
                self.assertEqual(rejected["receipts"], [])
                failures_after = self.routing_failures("doput", "lease_draining")
                self.assertTrue(
                    failures_after - failures_before == 1
                    or any(reason in rejected.get("error", "").lower() for reason in ("drain", "routing", "owner")),
                    rejected,
                )
                (self.experiment.output / "new-rpc-drain-rejection.json").write_text(json.dumps({
                    "router_lease_draining_failures_before": failures_before,
                    "router_lease_draining_failures_after": failures_after,
                    "transport_error": rejected.get("error"), "admission_receipts": rejected["receipts"],
                }, indent=2))
                admitted = source.result(timeout=40)
                result = move(self.experiment.root, key, record=record)
                (self.experiment.output / "bounded-drain-complete.json").write_text(json.dumps(result, indent=2))
                self.assertGreater(result["lease"]["fencing_epoch"], lease["fencing_epoch"])
                report = save_report(source_settings, [admitted], started)
                self.assertTrue(report["successful_pilot"], report["generation"])
        finally:
            self.experiment.cleanup_pressure()
            if source is not None and source.done():
                save_report(source_settings, [source.result()], started)

    def test_valid_canonical_native_flight_publishes_and_reconciles(self):
        started = time.monotonic()
        outcome = upload(self.settings, self.sdk, 0)
        report = save_report(self.settings, [outcome], started)
        self.assertTrue(report["successful_pilot"], report["generation"])


if __name__ == "__main__":
    unittest.main()
