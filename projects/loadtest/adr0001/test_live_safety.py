"""Deterministic private footprint tests; no native effects or qualification claims."""
import copy
import json
from pathlib import Path
import unittest

from jsonschema.exceptions import ValidationError

from live_contract import validate_packet
from live_safety import GuardEvidence, digest, footprint, native_safety


class SafetyFootprintTests(unittest.TestCase):
    def test_native_heartbeat_counters_are_diagnostic_but_identity_eligibility_and_policy_are_not(self):
        state = {
            "revision": 1,
            "members": [{"writer": {"collector": "c1", "process": "p1", "generation": 1},
                         "eligible": True, "endpoint": "http://c1:50051",
                         "pressure": 0.1, "rss_bytes": 1, "active": 0}],
            "assignments": [{"revision": 1, "record": {"key": "main", "epoch": 1,
                             "writer": {"collector": "c1", "process": "p1", "generation": 1},
                             "lifecycle": "Active", "pending": []}}],
            "policies": {}, "table_policies": {},
            "optimization": {"sampled_at": 1, "sustained": 1, "constraint": ""},
            "observations": {"c1": {"sequence": 1, "demand": {}, "capacity": {}, "tables": {},
                                     "allocation_revision": 1, "fingerprints": ["a"]}},
        }
        before = native_safety(state)
        changed = copy.deepcopy(state)
        changed["revision"] = 2
        changed["members"][0].update(pressure=0.9, rss_bytes=100, active=1)
        changed["observations"]["c1"].update(sequence=2, demand={"bytes": 100}, capacity={"rows": 200},
                                             allocation_revision=2)
        changed["assignments"][0]["revision"] = 2
        changed["optimization"].update(sampled_at=2, sustained=2, constraint="stale-capacity")
        self.assertEqual(native_safety(changed), before)
        for field, value in (("eligible", False), ("endpoint", "http://replacement"),
                             ("writer", {"collector": "c1", "process": "p2", "generation": 2}),
                             ("unclassified_fact", None)):
            changed = copy.deepcopy(state)
            changed["members"][0][field] = value
            self.assertNotEqual(native_safety(changed), before)
        changed = copy.deepcopy(state)
        changed["observations"]["c1"]["fingerprints"] = []
        self.assertNotEqual(native_safety(changed), before)
        changed["observations"]["c1"]["allocation_revision"] = None
        self.assertNotEqual(native_safety(changed), before)
        changed = copy.deepcopy(state)
        changed["assignments"][0]["record"]["epoch"] = 2
        self.assertNotEqual(native_safety(changed), before)
        changed = copy.deepcopy(state)
        changed["optimization"]["unclassified_rule"] = True
        self.assertNotEqual(native_safety(changed), before)
        self.assertEqual(state["revision"], 1)

    def test_guard_evidence_is_bounded_and_measures_failures_without_suppressing_them(self):
        evidence = GuardEvidence()
        def fail():
            raise RuntimeError("private message must not be recorded")
        with self.assertRaises(RuntimeError):
            evidence.measure("op-0", "native-view", fail)
        for index in range(70):
            evidence.measure("op-" + str(index), "config-read", lambda: None)
        snapshot = evidence.snapshot()
        self.assertEqual(len(snapshot["records"]), 65)
        self.assertGreater(snapshot["dropped"], 0)
        self.assertLessEqual(snapshot["bytes"], 65536)
        self.assertEqual(snapshot["records"][0]["outcome"], "RuntimeError")
        self.assertNotIn("private message", json.dumps(snapshot))
        large = GuardEvidence()
        for _ in range(1000):
            large.record("op-1", "final-guard", large.clock_origin, "stale", reasons=["x" * 100])
        self.assertLessEqual(large.snapshot()["bytes"], 65536)
        self.assertLessEqual(len(json.dumps(large.snapshot(), sort_keys=True).encode()), 65536)
        self.assertGreater(large.snapshot()["dropped"], 0)

    def test_exact_public_safety_fields_are_not_masked_by_diagnostic_filter(self):
        snapshot = {
            "bridge": {"id": "bridge", "incarnation": "boot"},
            "deployment": {"id": "deployment", "incarnation": "deployment-1"},
            "replicas": [{"resource": {"id": "c1", "incarnation": "container-1"}, "role": "collector",
                          "status": "active", "identity_verified": True, "running": True, "paused": False,
                          "writer": {"collector": "c1", "process": "p1", "generation": "1"},
                          "policy_revision": "1", "applied_policy_revision": "1",
                          "excluded": False, "block_streams": False, "retiring": False}],
            "tables": [{"id": "main", "epoch": "1", "lifecycle": "active",
                        "writer": {"collector": "c1", "process": "p1", "generation": "1"},
                        "recovery_obligations": []}],
            "coverage": {"missing": ["cpu"], "dropped": 0}, "metrics": [], "receipts": [],
            "receipt_observation_known": True, "freshness": {"status": "fresh"},
        }
        before = footprint(snapshot, {"qualified": True}, {"source": "qualified"})
        for field in snapshot["replicas"][0]:
            changed = copy.deepcopy(snapshot)
            changed["replicas"][0][field] = None if field != "resource" else {"id": "c1", "incarnation": "new"}
            with self.subTest(field=field):
                self.assertNotEqual(footprint(changed, {"qualified": True}, {"source": "qualified"}), before)
        for field in ("epoch", "lifecycle", "writer", "recovery_obligations"):
            changed = copy.deepcopy(snapshot)
            changed["tables"][0][field] = None
            with self.subTest(table_field=field):
                self.assertNotEqual(footprint(changed, {"qualified": True}, {"source": "qualified"}), before)
        self.assertNotEqual(footprint(snapshot, {"qualified": False}, {"source": "qualified"}), before)
        self.assertNotEqual(footprint(snapshot, {"qualified": True}, {"source": "changed"}), before)
        diagnostic = copy.deepcopy(snapshot)
        diagnostic.update(metrics=[{"value": "999"}], receipts=[{"completion": "accepted"}], revision="999")
        diagnostic["coverage"] = {"missing": ["memory", "cpu-counter-gap"], "dropped": 1}
        self.assertEqual(footprint(diagnostic, {"qualified": True}, {"source": "qualified"}), before)

    def test_footprint_input_is_bounded_and_nonfinite_values_are_not_sanitized(self):
        with self.assertRaises(ValueError):
            digest({"value": float("nan")})
        with self.assertRaises(ValueError):
            digest({"value": "x" * 1048577})

    def test_shared_contract_fixture_packets_and_missing_fence_regressions(self):
        path = Path(__file__).parent / "contracts/live-safety-revision-fixtures.json"
        fixtures = json.loads(path.read_text())
        for packet in fixtures["packets"]:
            with self.subTest(kind=packet["kind"]):
                validate_packet(packet)
        for packet in fixtures["packets"]:
            if packet["kind"] in ("select", "arm", "command"):
                for field in ("bridge", "safety_revision"):
                    old = copy.deepcopy(packet)
                    del old[field]
                    with self.subTest(kind=packet["kind"], field=field), self.assertRaises(ValidationError):
                        validate_packet(old)


if __name__ == "__main__":
    unittest.main()
