import copy
from dataclasses import replace
import json
import unittest

from jsonschema.exceptions import ValidationError

from live_contract import (
    ARM_TTL_NS, SNAPSHOT_AGE_NS, Denial, DeploymentFence, SessionFence,
    authorize_command, decode_request, validate_packet,
)


class LiveContractTests(unittest.TestCase):
    def setUp(self):
        self.identity = {"id": "poc", "incarnation": "deployment-1"}
        self.target = {
            "resource": {"id": "collector1", "incarnation": "container-start-1"},
            "writer": {"collector": "collector1", "process": "process-1", "generation": "generation-1"},
            "policy_revision": "3",
        }
        self.packet = {
            "kind": "command", "schema_version": 1, "operation_id": "operation-1",
            "selection_id": "selection-1", "arm_id": "arm-1", "deployment": self.identity,
            "revision": "7", "action": {"operation": "exclusion", "target": self.target, "enabled": True},
        }
        self.now = 100_000_000_000
        self.deployment = DeploymentFence(
            self.identity, "7", self.now, True, True, frozenset({"exclusion"}),
            {"collector1": self.target},
        )
        self.session = SessionFence(
            "http://127.0.0.1:8080", True, True, "live", self.identity,
            "selection-1", "arm-1", self.now + ARM_TTL_NS,
        )

    def authorize(self, **kwargs):
        return authorize_command(
            kwargs.get("packet", self.packet), kwargs.get("deployment", self.deployment),
            kwargs.get("session", self.session), kwargs.get("origin", self.session.allowed_origin),
            kwargs.get("now_ns", self.now),
        )

    def test_exact_fresh_qualified_server_owned_fences_allow_policy_validation_only(self):
        self.assertEqual(self.authorize(), self.packet)

    def test_no_default_connection_or_recorded_authority(self):
        for session in (SessionFence(self.session.allowed_origin), replace(self.session, mode="recorded"),
                        replace(self.session, mode="simulation")):
            with self.subTest(session=session), self.assertRaises(Denial):
                self.authorize(session=session)
        with self.assertRaisesRegex(Denial, "disconnected"):
            self.authorize(deployment=replace(self.deployment, connected=False))

    def test_authentication_origin_csrf_and_qualification_are_independent_denials(self):
        for updates, code in (({"authenticated": False}, "unauthenticated"),
                              ({"csrf_verified": False}, "csrf")):
            with self.subTest(code=code), self.assertRaisesRegex(Denial, code):
                self.authorize(session=replace(self.session, **updates))
        with self.assertRaisesRegex(Denial, "origin"):
            self.authorize(origin="http://localhost:8080")
        with self.assertRaisesRegex(Denial, "origin"):
            self.authorize(session=replace(self.session, allowed_origin="https://external.example:443"))
        with self.assertRaisesRegex(Denial, "unqualified"):
            self.authorize(deployment=replace(self.deployment, qualified=False))

    def test_unknown_stale_future_or_wrong_revision_snapshot_is_denied(self):
        for updates in ({"observed_ns": None}, {"observed_ns": self.now - SNAPSHOT_AGE_NS - 1},
                        {"observed_ns": self.now + 1}, {"revision": "8"}):
            with self.subTest(updates=updates), self.assertRaisesRegex(Denial, "stale"):
                self.authorize(deployment=replace(self.deployment, **updates))

    def test_expired_wrong_or_excessive_arm_is_denied(self):
        for updates in ({"arm_deadline_ns": self.now}, {"arm_deadline_ns": self.now + ARM_TTL_NS + 1},
                        {"arm_id": "old-arm"}, {"selection_id": "old-selection"}):
            with self.subTest(updates=updates), self.assertRaisesRegex(Denial, "disarmed"):
                self.authorize(session=replace(self.session, **updates))

    def test_resource_writer_policy_or_deployment_replacement_is_denied(self):
        for path, replacement in ((("resource", "incarnation"), "container-start-2"),
                                  (("writer", "generation"), "generation-2"),
                                  (("writer", "process"), "process-2"),
                                  (("policy_revision",), "4")):
            packet = copy.deepcopy(self.packet)
            target = packet["action"]["target"]
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = replacement
            with self.subTest(path=path), self.assertRaisesRegex(Denial, "wrong-target"):
                self.authorize(packet=packet)
        with self.assertRaisesRegex(Denial, "wrong-target"):
            self.authorize(deployment=replace(self.deployment, identity={"id": "poc", "incarnation": "deployment-2"}))

    def test_unsupported_is_not_enabled_by_valid_shape(self):
        with self.assertRaisesRegex(Denial, "unsupported"):
            self.authorize(deployment=replace(self.deployment, supported=frozenset()))

    def test_unknown_fields_shell_targets_and_out_of_bound_work_are_rejected(self):
        packet = copy.deepcopy(self.packet)
        packet["action"]["shell"] = "test-forbidden"
        with self.assertRaises(ValidationError):
            validate_packet(packet)
        packet["action"] = {"operation": "workload", "profile": "small", "requests": 257, "duration_ms": 1}
        with self.assertRaises(ValidationError):
            validate_packet(packet)

    def test_request_duplicate_keys_and_size_are_bounded(self):
        raw = json.dumps(self.packet).encode()
        self.assertEqual(decode_request(raw), self.packet)
        with self.assertRaises(ValueError):
            decode_request(b'{"kind":"command","kind":"select"}')
        with self.assertRaisesRegex(Denial, "limit"):
            decode_request(b" " * 32769)

    def test_gap_requires_empty_feed_and_resnapshot(self):
        packet = {
            "kind": "feed", "schema_version": 1, "bridge": {"id": "bridge", "incarnation": "bridge-1"},
            "deployment": self.identity, "after": "4", "next": "7", "gap": True,
            "resnapshot": True, "items": [],
        }
        validate_packet(packet)
        packet["resnapshot"] = False
        with self.assertRaisesRegex(Denial, "invalid"):
            validate_packet(packet)

    def snapshot(self):
        return {
            "kind": "snapshot", "schema_version": 1,
            "bridge": {"id": "bridge", "incarnation": "bridge-1"}, "deployment": self.identity, "revision": "8",
            "freshness": {"status": "fresh", "age_ns": "0", "maximum_age_ns": "5000000000"},
            "replicas": [{
                "resource": self.target["resource"], "role": "collector", "status": "active",
                "writer": self.target["writer"], "policy_revision": "3", "applied_policy_revision": "3",
                "excluded": False, "block_streams": False, "retiring": False,
            }],
            "tables": [], "receipts": [], "metrics": [{
                "id": "cpu-sample-1", "resource": self.target["resource"],
                "process_id": "process-1", "clock_id": "clock-1", "offset_ns": "0",
                "duration_ns": "1000", "metric": "cpu", "unit": "ns", "value": "23",
                "evidence": "measured", "method": "process-counter",
            }],
            "coverage": {"complete": False, "missing": ["service-timing"], "dropped": 0},
        }

    def test_metrics_require_closed_units_exact_resources_and_explicit_unknowns(self):
        packet = self.snapshot()
        validate_packet(packet)
        packet["metrics"][0].update(value=None, evidence="unavailable", method="not-observed")
        validate_packet(packet)
        packet["metrics"][0]["value"] = "0"
        with self.assertRaises(ValidationError):
            validate_packet(packet)
        packet = self.snapshot()
        packet["metrics"][0]["unit"] = "bytes"
        with self.assertRaises(ValidationError):
            validate_packet(packet)
        packet = self.snapshot()
        packet["metrics"][0]["resource"] = {"id": "collector1", "incarnation": "replaced"}
        with self.assertRaisesRegex(Denial, "invalid"):
            validate_packet(packet)

    def test_feed_revisions_are_consecutive_and_empty_next_matches_after(self):
        packet = {
            "kind": "feed", "schema_version": 1, "bridge": {"id": "bridge", "incarnation": "bridge-1"},
            "deployment": self.identity, "after": "7", "next": "8", "gap": False,
            "resnapshot": False, "items": [self.snapshot()],
        }
        validate_packet(packet)
        packet["after"] = "6"
        with self.assertRaisesRegex(Denial, "invalid"):
            validate_packet(packet)
        packet["items"] = []
        with self.assertRaisesRegex(Denial, "invalid"):
            validate_packet(packet)


if __name__ == "__main__":
    unittest.main()
