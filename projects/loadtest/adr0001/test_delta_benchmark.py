"""Focused finite-client half-close and conservative statistics checks."""
import json
from pathlib import Path
import unittest
import uuid

import pyarrow as pa
import pyarrow.flight as flight
from deltalake import write_deltalake

from delta_benchmark import Benchmark, REPO, distribution


class FiniteClient(unittest.TestCase):
    def test_half_closes_input_before_waiting_for_completion(self):
        root = REPO / "target/delta-benchmark/checks" / ("client-eof-" + uuid.uuid4().hex)
        destination = root / "delta-private/tables/main"
        destination.parent.mkdir(parents=True)
        class Server(flight.FlightServerBase):
            def do_put(self, context, descriptor, reader, writer):
                batches = [chunk.data for chunk in reader]
                write_deltalake(str(destination), pa.Table.from_batches(batches))
                writer.write(pa.py_buffer(json.dumps({"kind": "DELTA_OPERATION", "final": False,
                                                       "operation": "test-eof"}).encode()))
                writer.write(pa.py_buffer(json.dumps({"kind": "DELTA_COMMITTED", "operation": "test-eof",
                                                       "version": 0, "digest": "test-only"}).encode()))
        server = Server("grpc://127.0.0.1:0")
        try:
            runtime = Benchmark.__new__(Benchmark)
            runtime.root = root
            runtime.config = {"edge_secret": "client-test"}
            runtime.schemas = [pa.schema([pa.field("id", pa.int64(), nullable=False)])]
            runtime.fingerprints = ["client-test"]
            result = runtime.put((100, 3, "main", "test", server.port))
            self.assertEqual(result["terminal"], "final")
            self.assertEqual(result["completion"], "stream-final")
            self.assertEqual(result["payload_retries"], 0)
            self.assertGreaterEqual(result["reader_visibility_ms"], result["terminal_ms"])
        finally:
            server.shutdown()

    def test_empty_and_finite_samples_do_not_claim_supported_tails(self):
        self.assertEqual(distribution([])["n"], 0)
        self.assertIsNone(distribution([])["p50"])
        small = distribution([1, 2, 3, 4])
        self.assertEqual(small["p50"], 2.5)
        self.assertIsNone(small["p95"])
        self.assertIsNone(small["p99"])
        self.assertAlmostEqual(small["exploratory_p95"], 3.85)
        self.assertIsNotNone(distribution(range(100))["p95"])
        self.assertIsNone(distribution(range(100))["p99"])


if __name__ == "__main__":
    unittest.main()
