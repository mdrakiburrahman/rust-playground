"""Integrated creator/observer/attachment/fault fixtures; not native effect qualification."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import live_deployment
from live_capacity import CollectorCapacity
from live_contract import Denial
from live_effects import EffectExecutor
from live_deployment import AttachedRuntime, MANIFEST, save_deployment
from live_faults import FaultResource
from live_inventory import FleetInventory, read_inventory
from live_ledger import OperationLedger
from live_runtime import RuntimeObserver
from live_server import Bridge


class CapacityIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.repo = Path(self.directory.name)
        self.project = "publication-gate-synthetic-capacity"
        self.root = self.repo / "target/publication-gate" / self.project
        self.root.mkdir(parents=True)
        self.root.chmod(0o700)
        for directory in ("bin", "config", "staging", "delta-private/tables"):
            (self.root / directory).mkdir(parents=True)
        (self.repo / "target/debug").mkdir(parents=True)
        for base in (self.root / "bin", self.repo / "target/debug"):
            (base / "adr-flight").write_bytes(b"synthetic qualified binary")
        self.deployment = {"id": self.project, "incarnation": "f" * 32}
        self.config = {"operator_token": "synthetic-operator", "tables": [{"key": "main"}],
                       "collectors": {"collector" + str(i): "synthetic-token" + str(i) for i in range(1, 5)}}
        (self.root / "config/controller.json").write_text(json.dumps(self.config))
        (self.root / "config/collector4.json").write_text(json.dumps({
            "operator_token": "", "gateway_token": "", "etcd": [], "etcd_password": "",
            "storage": "/stage", "collectors": {"collector4": "synthetic-token4"},
            "tables": self.config["tables"], "delta": {"publisher": "http://publisher:50061"},
        }))
        roles = ("controller", "gateway", "publisher", "sql", "collector", "collector1", "collector2", "collector3")
        self.states = {
            f"{index:064x}": {
                "Id": f"{index:064x}", "Name": "/" + self.project + "-" + role, "Image": "sha256:" + "a" * 64,
                "Config": {"Labels": {"delta-runtime": self.project}},
                "State": {"Running": role != "collector", "Paused": False, "StartedAt": "synthetic-start"},
            } for index, role in enumerate(roles, 1)
        }
        self.engine = SimpleNamespace(get=self.get)
        birth = SimpleNamespace(
            root=self.root, project=self.project, subnet="10.200.219.0/24", arm="adr",
            names={role: self.project + "-" + role for role in roles if role not in ("sql", "collector")},
            container_ids={state["Name"][1:]: identity for identity, state in self.states.items()},
            control_role="controller", control_port=18001,
        )
        self.patcher = patch.object(live_deployment, "REPO", self.repo)
        self.patcher.start()
        save_deployment(birth, self.deployment, None, self.engine)
        self.manifest = (self.root / MANIFEST).read_bytes()
        self.runtime = self.attach()
        self.inventory = FleetInventory(self.root, self.deployment)
        self.view = {"members": [{"writer": {"collector": "collector" + str(i), "process": "process" + str(i),
                                            "generation": 1}, "eligible": True} for i in range(1, 4)],
                     "policies": {}, "assignments": [], "observations": {}}
        self.runtime.post = lambda _: copy.deepcopy(self.view)
        frontend = self.repo / "frontend"
        frontend.mkdir()
        (frontend / "index.html").write_text("synthetic")
        self.ledger = OperationLedger(self.root / "test-operations.sqlite")
        self.bridge = Bridge("http://127.0.0.1:18002", self.deployment, frontend, self.ledger)
        self.observer = RuntimeObserver(self.runtime, self.bridge, self.engine, lambda: ([], [], 0))
        self.observer.authority = lambda: copy.deepcopy(self.view)
        self.observer.sample()
        self.capacity = CollectorCapacity(self.runtime, self.inventory, self.bridge.snapshot)
        self.creates = 0
        self.command = None
        self.capacity.create = self.create

    def tearDown(self):
        self.observer.close()
        self.inventory.close()
        self.ledger.close()
        self.patcher.stop()
        self.directory.cleanup()

    def attach(self):
        return AttachedRuntime(self.project, "10.200.219.0/24", "adr", None, self.engine)

    def get(self, path):
        if path.startswith("/containers/json?"):
            return [{"Id": identity} for identity, state in self.states.items()
                    if "bridge-creation-nonce" in state["Config"]["Labels"]]
        if "/stats?" in path:
            return {"cpu_stats": {"cpu_usage": {"total_usage": 10}}, "memory_stats": {"usage": 4096}}
        return copy.deepcopy(self.states[path.split("/")[2]])

    def create(self, command):
        self.creates += 1
        self.command = command
        item = read_inventory(self.root, self.deployment)[0]
        path = self.root / "config" / item["dispatch"]["path"]
        labels = {"delta-runtime": self.project, "bridge-deployment-incarnation": self.deployment["incarnation"],
                  "bridge-creation-nonce": item["nonce"]}
        self.states["9" * 64] = {
            "Id": "9" * 64, "Name": "/" + item["name"], "Image": item["image"],
            "Config": {"Labels": labels, "User": "21001:21001",
                       "Cmd": ["/native/adr-flight", "--config", "/config.json", "--collector", "collector4"]},
            "State": {"Running": True, "Paused": False, "StartedAt": "synthetic-created-start"},
            "HostConfig": {"NetworkMode": self.project, "Privileged": False, "ReadonlyRootfs": True,
                           "NanoCpus": 1_000_000_000, "Memory": 1_073_741_824, "PidsLimit": 256,
                           "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"]},
            "NetworkSettings": {"Networks": {self.project: {"Aliases": ["collector4"]}}},
            "Mounts": [{"Type": "bind", "Source": str(source), "Destination": destination, "RW": writable}
                       for destination, source, writable in (
                           ("/native", self.root / "bin", False), ("/config.json", path, False),
                           ("/stage", self.root / "staging/collector4", True),
                           ("/destination", self.root / "delta-private/tables", False))],
        }
        raise RuntimeError("synthetic lost Docker response")

    def dispatch(self):
        return self.capacity.run("add-four", {"operation": "replicas", "role": "collector", "desired": 4},
                                 lambda: None, lambda: None)

    def guarded_command(self):
        self.executor = EffectExecutor(self.ledger, self.runtime.post, lambda _: None,
                                       self.bridge.snapshot, capacity=self.capacity, policy_enabled=False)
        self.bridge.executor = self.executor
        self.capacity.evidence = self.bridge.evidence
        self.observer.sample()
        _, token = self.bridge.bootstrap({"grant": self.bridge.grant})
        session = self.bridge.sessions[token]
        snapshot = self.bridge.snapshot()
        reference = {name: snapshot[name] for name in ("bridge", "deployment", "revision", "safety_revision")}
        selection = self.bridge.select({"kind": "select", "schema_version": 1, **reference}, session)
        armed = self.bridge.arm({"kind": "arm", "schema_version": 1, **reference,
                                "selection_id": selection["selection_id"], "confirm": "dedicated-local-poc"}, session)
        return {
            "kind": "command", "schema_version": 1, "operation_id": "guarded-add",
            **reference, "selection_id": selection["selection_id"], "arm_id": armed["arm_id"],
            "action": {"operation": "replicas", "role": "collector", "desired": 4},
        }, session

    def guarded_result(self, packet, session):
        try:
            self.bridge.command(packet, session)
            self.assertTrue(self.executor.lock.acquire(timeout=3))
            self.executor.lock.release()
            return self.ledger.get(packet["operation_id"], self.deployment)
        finally:
            self.executor.close()

    def test_diagnostic_sample_after_own_inventory_intent_does_not_cancel_creator(self):
        packet, session = self.guarded_command()
        original = self.capacity.prepare
        def prepare(operation_id):
            command = original(operation_id)
            # The observer sees the durable unbound intent, but cannot fabricate a resource.
            self.observer.sample()
            return command
        self.capacity.prepare = prepare
        creates = []
        self.capacity.create = lambda command: creates.append(command)
        result = self.guarded_result(packet, session)
        self.assertEqual(len(creates), 1)
        self.assertEqual(result["status"], "unknown")  # No simulated container or receipt was fabricated.
        self.assertIsNotNone(read_inventory(self.root, self.deployment)[0]["dispatch"])
        self.assertEqual(result["requested_safety_revision"], packet["safety_revision"])
        evidence = self.bridge.evidence.snapshot()
        self.assertTrue(any(row["phase"] == "final-guard" and row["outcome"] == "allowed"
                            for row in evidence["records"]))
        self.assertTrue(any(row["phase"] == "dispatch-marker" for row in evidence["records"]))

    def test_policy_change_during_creator_preflight_cancels_without_dispatch(self):
        packet, session = self.guarded_command()
        original = self.capacity.prepare
        def prepare(operation_id):
            command = original(operation_id)
            self.view["policies"]["collector1"] = {
                "incarnation": self.view["members"][0]["writer"], "revision": 1, "applied_revision": 0,
                "excluded": True, "block_streams": False, "retiring": False,
            }
            self.observer.sample()
            return command
        self.capacity.prepare = prepare
        result = self.guarded_result(packet, session)
        self.assertEqual((result["status"], result["error_code"]), ("rejected", "stale"))
        self.assertEqual(self.creates, 0)
        self.assertEqual(read_inventory(self.root, self.deployment), [])
        self.assertEqual(self.inventory.connection.execute("SELECT COUNT(*) FROM fleet_cancellations").fetchone()[0], 1)
        self.assertEqual(self.inventory.connection.execute("SELECT COUNT(*) FROM fleet_dispatches").fetchone()[0], 0)

    def test_original_observation_age_expires_during_creator_prepare_without_renewal(self):
        packet, session = self.guarded_command()
        original = self.capacity.prepare
        def prepare(operation_id):
            command = original(operation_id)
            self.observer.sample()
            for index, entry in enumerate(self.bridge.history):
                if entry[0]["revision"] == packet["revision"]:
                    self.bridge.history[index] = (entry[0], time.monotonic_ns() - 6_000_000_000, True)
            return command
        self.capacity.prepare = prepare
        result = self.guarded_result(packet, session)
        self.assertEqual((result["status"], result["error_code"]), ("rejected", "stale"))
        guard = next(row for row in self.bridge.evidence.snapshot()["records"] if row["phase"] == "final-guard")
        self.assertEqual(guard["reasons"], ["request-observation-age"])
        self.assertEqual(self.creates, 0)
        self.assertEqual(read_inventory(self.root, self.deployment), [])

    def test_inventory_change_between_admission_and_reservation_cannot_use_new_baseline(self):
        packet, session = self.guarded_command()
        self.inventory.reserve_creation("foreign-preflight", "collector", 4, self.inventory.snapshot()["revision"])
        try:
            with self.assertRaisesRegex(Denial, "stale"):
                self.bridge.command(packet, session)
            with self.assertRaisesRegex(Denial, "unknown-operation"):
                self.ledger.get(packet["operation_id"], self.deployment)
        finally:
            self.executor.close()
        self.assertEqual(self.creates, 0)

    def assert_prepare_phase_schedule(self, phase, critical=False):
        packet, session = self.guarded_command()
        original = self.capacity.evidence.measure
        injected = []
        def measure(operation_id, current_phase, function, *args, **kwargs):
            result = original(operation_id, current_phase, function, *args, **kwargs)
            if current_phase == phase and not injected:
                injected.append(phase)
                if critical:
                    self.view["unclassified_safety_change"] = True
                self.observer.sample()
            return result
        self.capacity.evidence.measure = measure
        dispatched = []
        self.capacity.create = lambda command: dispatched.append(command)
        result = self.guarded_result(packet, session)
        self.assertEqual(injected, [phase])
        if critical:
            self.assertEqual((result["status"], result["error_code"]), ("rejected", "stale"))
            self.assertEqual(dispatched, [])
            self.assertEqual(read_inventory(self.root, self.deployment), [])
        else:
            self.assertEqual(len(dispatched), 1)
            self.assertEqual(result["status"], "unknown")

    def test_diagnostic_publish_between_native_view_and_resource_checks_allows_original_dispatch(self):
        self.assert_prepare_phase_schedule("native-view")

    def test_diagnostic_publish_during_parallel_resource_checks_allows_original_dispatch(self):
        self.assert_prepare_phase_schedule("exact-resource-collector2")

    def test_diagnostic_publish_after_config_fsync_allows_original_dispatch(self):
        self.assert_prepare_phase_schedule("config-hardlink-fsync")

    def test_unknown_native_change_after_native_view_denies_dispatch(self):
        self.assert_prepare_phase_schedule("native-view", critical=True)

    def test_unknown_native_change_during_resource_checks_denies_dispatch(self):
        self.assert_prepare_phase_schedule("exact-resource-collector2", critical=True)

    def test_unknown_native_change_after_config_fsync_denies_dispatch(self):
        self.assert_prepare_phase_schedule("config-hardlink-fsync", critical=True)

    def ready(self):
        self.view["members"].append({"writer": {"collector": "collector4", "process": "process4", "generation": 1},
                                     "eligible": True})
        self.view["observations"]["collector4"] = {}
        self.observer.sample()

    def test_lost_creation_response_reconciles_without_second_create_and_projects_real_binding(self):
        with self.assertRaisesRegex(RuntimeError, "lost Docker response"):
            self.dispatch()
        self.assertEqual(self.capacity.outcome("add-four"), ("pending", None))
        self.ready()
        self.assertEqual(self.capacity.outcome("add-four")[0], "completed")
        self.assertEqual(self.dispatch()[0], "completed")
        self.assertEqual(self.creates, 1)
        self.assertIn("--pull=never", self.command)
        self.assertEqual((self.root / MANIFEST).read_bytes(), self.manifest)
        self.assertFalse(self.capacity.available())
        attached = self.attach()
        self.assertEqual(attached.records[attached.names["collector4"]]["id"], "9" * 64)
        row = next(row for row in self.bridge.snapshot()["replicas"] if row["resource"]["id"] == "resource-collector4")
        self.assertIs(row["identity_verified"], True)
        self.assertEqual(row["status"], "active")

    def test_unknown_creation_is_owned_for_exact_cleanup_but_never_fabricated_as_ready(self):
        with self.assertRaises(RuntimeError):
            self.dispatch()
        self.runtime.refresh_inventory()
        self.assertNotIn("collector4", self.runtime.names)
        self.assertTrue(self.runtime.inventory_uncertain)
        self.runtime.refresh_inventory(cleanup=True)
        self.assertIn("9" * 64, self.runtime.container_ids.values())
        self.assertNotIn("collector4", self.runtime.names)

    def test_definitive_predispatch_disarm_releases_slot_without_forgetting_rejected_identity(self):
        def disarm():
            raise Denial("disarmed")
        with self.assertRaisesRegex(Denial, "disarmed"):
            self.capacity.run("rejected-add", {"operation": "replicas", "role": "collector", "desired": 4},
                              disarm, lambda: None)
        self.assertEqual(self.creates, 0)
        self.assertEqual(read_inventory(self.root, self.deployment), [])
        self.assertTrue(self.inventory.reserve_creation("new-confirmed-add", "collector", 4,
                                                        self.inventory.snapshot()["revision"]))

    def test_isolation_and_changed_exact_incarnation_cannot_become_binding_or_readiness(self):
        with self.assertRaises(RuntimeError):
            self.dispatch()
        self.states["9" * 64]["HostConfig"]["NetworkMode"] = "host"
        with self.assertRaisesRegex(Denial, "wrong-target"):
            self.capacity.outcome("add-four")
        self.assertIsNone(read_inventory(self.root, self.deployment)[0]["record"])
        self.states["9" * 64]["HostConfig"]["NetworkMode"] = self.project
        self.capacity.outcome("add-four")
        self.states["9" * 64]["State"]["StartedAt"] = "replaced"
        with self.assertRaisesRegex(Denial, "wrong-target"):
            self.capacity.outcome("add-four")

    def test_selected_dynamic_fault_preflight_reads_overlay_but_repair_survives_missing_inventory(self):
        with self.assertRaises(RuntimeError):
            self.dispatch()
        self.capacity.outcome("add-four")
        item = read_inventory(self.root, self.deployment)[0]
        record = {"intent": {"deployment": self.deployment, "action": {
            "operation": "fault", "resource": {"id": "resource-collector4", "incarnation": hashlib.sha256(
                ("9" * 64 + "synthetic-created-start").encode()).hexdigest()},
            "fault": "pause", "duration_ms": 500,
        }}, "native": {**item["record"], "name": item["name"],
                       "creation_labels": self.states["9" * 64]["Config"]["Labels"]}}
        with patch("live_faults.deployment_root", return_value=self.root):
            FaultResource(self.project, self.runtime.subnet, "adr", record, self.engine)
            self.inventory.close()
            self.inventory = FleetInventory(self.root, self.deployment)
            path = self.root / "bridge-fleet.sqlite"
            path.rename(self.root / "displaced-fleet.sqlite")
            with self.assertRaises(Denial):
                FaultResource(self.project, self.runtime.subnet, "adr", record, self.engine)
            repaired = FaultResource(self.project, self.runtime.subnet, "adr", record, self.engine, repair_only=True)
            self.assertEqual(repaired.owned_container(item["name"]), "9" * 64)


if __name__ == "__main__":
    unittest.main()
