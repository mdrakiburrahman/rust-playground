"""Approved focused retained-client tests, using real local Flight RPCs and dictionary decoding."""
import json
from pathlib import Path
import sys
import unittest

import pyarrow as pa
import pyarrow.flight as flight

sys.path.insert(0, str(Path(__file__).parent))
from benchmark import retained_upload
from observation import ClientAccounting, cpu_interval, read_ns


class Server(flight.FlightServerBase):
    def __init__(self):
        super().__init__("grpc://127.0.0.1:0")
        self.seen = []
        self.schemas = []
        self.reject = False
        self.stale_once = False

    def do_put(self, context, descriptor, reader, writer):
        if self.reject:
            raise flight.FlightUnauthenticatedError("unauthorized schema/table")
        if self.stale_once:
            self.stale_once = False
            raise flight.FlightUnavailableError("NOT_OWNER")
        self.schemas.append(reader.schema)
        for chunk in reader:
            self.seen.extend(chunk.data.column(0).to_pylist())
            writer.write(pa.py_buffer(json.dumps({"kind": "TABLE_COMMITTED", "head": {"commit": str(len(self.seen))}}).encode()))


class Retention(unittest.TestCase):
    def setUp(self):
        self.server = Server()
        self.schema = pa.schema([pa.field("sql_server_instance_name", pa.string(), nullable=False)])
        self.manifest = {"table": {"catalog": "c", "schema": "s", "table": "t", "table_id": "immutable",
                                  "fingerprint": "registered"},
                         "flight": {"descriptor_path": ["c", "s", "t", "v1"]}}
        self.settings = {"run_id": "finite-unit", "rows": 3, "batches_per_stream": 1, "max_attempts": 3}

    def tearDown(self):
        self.server.shutdown()

    def test_lost_reply_retains_complete_input_and_counts_duplicate_attempt(self):
        outcome = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, self.schema), self.settings, 0, lose_first_reply=True)
        self.assertEqual(outcome["terminal"], "ok")
        self.assertEqual(len(outcome["attempts"]), 2)
        self.assertEqual(len(self.server.seen), 6)
        self.assertEqual(set(self.server.seen), set(outcome["markers"]))
        self.assertEqual(outcome["retained_bytes"], 0)

    def test_permanent_auth_failure_is_not_replayed_and_input_remains_retained(self):
        self.server.reject = True
        outcome = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, self.schema), self.settings, 0)
        self.assertEqual(outcome["terminal"], "unresolved")
        self.assertEqual(len(outcome["attempts"]), 1)
        self.assertGreater(outcome["retained_bytes"], 0)
        self.assertEqual(self.server.seen, [])

    def test_dictionary_is_reconstructed_on_each_uncertain_replay(self):
        schema = pa.schema([pa.field("sql_server_instance_name", pa.string(), nullable=False),
                            pa.field("tag", pa.dictionary(pa.int8(), pa.string()), nullable=False)])
        outcome = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, schema), self.settings, 0, lose_first_reply=True)
        self.assertEqual(outcome["terminal"], "ok")
        self.assertEqual(self.server.schemas, [schema, schema])
        self.assertEqual(len(self.server.seen), 6)

    def test_exhaustion_returns_complete_arrow_retry_handle_separate_from_json_evidence(self):
        self.server.reject = True
        outcome = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, self.schema), self.settings, 0)
        handle = outcome.retry_handle
        self.assertIsNotNone(handle)
        self.assertEqual(sum(b.nbytes for b in handle.batches), outcome["retained_bytes"])
        self.assertEqual(handle.batches[0].schema, self.schema)
        json.dumps(outcome)
        self.server.reject = False
        replay = handle.resume(f"grpc://127.0.0.1:{self.server.port}", b"s"*32)
        self.assertEqual(replay["terminal"], "ok")
        self.assertEqual(self.server.seen, outcome["markers"])
        self.assertIsNone(replay.retry_handle)
        self.assertIn("stream_open_ms", replay["attempts"][0])
        self.assertIsNone(replay["attempts"][0]["admission_ms"])

    def test_stale_owner_is_transient_without_gateway_replay(self):
        self.server.stale_once = True
        outcome = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, self.schema), self.settings, 0)
        self.assertEqual(outcome["terminal"], "ok")
        self.assertEqual(len(outcome["attempts"]), 2)
        self.assertEqual(len(self.server.seen), 3)

    def test_finite_transient_exhaustion_keeps_dictionary_input_for_later_resume(self):
        self.server.stale_once = True
        schema = pa.schema([pa.field("sql_server_instance_name", pa.string(), nullable=False),
                            pa.field("tag", pa.dictionary(pa.int8(), pa.string()), nullable=False)])
        outcome = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, schema), dict(self.settings, max_attempts=1), 0)
        self.assertEqual(outcome["terminal"], "unresolved")
        self.assertEqual(len(outcome["attempts"]), 1)
        resumed = outcome.retry_handle.resume(f"grpc://127.0.0.1:{self.server.port}", b"s"*32)
        self.assertEqual(resumed["terminal"], "ok")
        self.assertEqual(self.server.schemas, [schema])
        self.assertEqual(set(self.server.seen), set(outcome["markers"]))

    def test_observation_counts_retention_once_across_retry_and_preserves_unresolved_bytes(self):
        counters = ClientAccounting(2, "adr")
        success = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, self.schema), self.settings, 0, lose_first_reply=True, accounting=counters)
        current = counters.sample()
        self.assertEqual(current["retained_bytes"], 0)
        self.assertEqual(current["high_water"]["retained_bytes"], success["arrow_bytes"])
        self.assertEqual(current["high_water"]["in_flight_bytes"], success["arrow_bytes"])
        self.assertEqual(current["queued_jobs"], 1)
        self.server.reject = True
        failed = retained_upload(f"grpc://127.0.0.1:{self.server.port}", b"s"*32,
            (self.manifest, self.schema), self.settings, 1, accounting=counters)
        current = counters.sample()
        self.assertEqual(current["unresolved_retained_bytes"], failed["arrow_bytes"])
        self.assertEqual(current["active_units"], 0)
        self.assertEqual(current["in_flight_bytes"], 0)
        self.assertEqual(current["queued_jobs"], 0)

    def test_cpu_observation_uses_elapsed_counter_interval_and_rejects_reset_or_duplicate_read(self):
        self.assertEqual(read_ns("2026-10-26T16:47:01.263397966Z") -
                         read_ns("2026-10-26T16:47:01.263397965+00:00"), 1)
        self.assertEqual(read_ns("2026-10-26T16:47:01.263Z") -
                         read_ns("2026-10-26T16:47:01Z"), 263_000_000)
        previous = {"observed_monotonic_ns": 1_000_000_000, "observer_clock_id": "observer-test",
                    "cpu_total_usage_ns": 700}
        current = {"observed_monotonic_ns": 2_000_000_000, "observer_clock_id": "observer-test",
                   "cpu_total_usage_ns": 500_000_700}
        self.assertEqual(cpu_interval(previous, current)["one_core_percent"], 50)
        self.assertIsNone(cpu_interval(None, current))
        with self.assertRaisesRegex(ValueError, "counter/clock"):
            cpu_interval(current, current)
        with self.assertRaisesRegex(ValueError, "counter/clock"):
            cpu_interval(current, {**current, "observed_monotonic_ns": 3_000_000_000, "cpu_total_usage_ns": 0})
        with self.assertRaisesRegex(ValueError, "crosses observer clocks"):
            cpu_interval(previous, {**current, "observer_clock_id": "foreign-observer"})


if __name__ == "__main__":
    unittest.main()
