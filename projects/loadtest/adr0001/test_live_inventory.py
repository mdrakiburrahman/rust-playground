"""Synthetic persistence/identity fixtures; no runtime creation or staff qualification."""
import copy
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import Mock

from live_contract import Denial
from live_deployment import MANIFEST, write_private_new
from live_inventory import FleetInventory


class FleetInventoryTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.root.chmod(0o700)
        self.project = "publication-gate-synthetic-fleet"
        self.deployment = {"id": self.project, "incarnation": "f" * 32}
        self.baseline = {
            self.project + "-" + name: {
                "id": f"{index:064x}", "started_at": "synthetic-exact-start",
                "image": "sha256:" + "a" * 64,
            }
            for index, name in enumerate(
                ("controller", "gateway", "publisher", "sql", "collector",
                 "collector1", "collector2", "collector3"), 1)
        }
        write_private_new(self.root / MANIFEST, {
            "version": 1, "project": self.project, "deployment": self.deployment,
            "containers": self.baseline,
        })
        self.manifest = (self.root / MANIFEST).read_bytes()
        self.inventory = FleetInventory(self.root, self.deployment)

    def tearDown(self):
        if self.inventory is not None:
            self.inventory.close()
        self.directory.cleanup()

    def reopen(self, **kwargs):
        self.inventory.close()
        self.inventory = None
        self.inventory = FleetInventory(self.root, self.deployment, **kwargs)

    def candidate(self, operation="create-four", identity="9" * 64):
        intent = next(item for item in self.inventory.snapshot()["creations"]
                      if item["operation_id"] == operation)
        state = {
            "Id": identity, "Name": "/" + intent["name"], "Image": intent["image"],
            "Config": {"Labels": {
                "delta-runtime": self.project,
                "bridge-deployment-incarnation": self.deployment["incarnation"],
                "bridge-creation-nonce": intent["nonce"],
            }},
            "State": {"Running": True, "Paused": False, "StartedAt": "synthetic-created-start"},
        }
        def get(path):
            if path.startswith("/containers/json?"):
                return [{"Id": identity}]
            self.assertEqual(path, f"/containers/{identity}/json")
            return copy.deepcopy(state)
        return state, Mock(get=Mock(side_effect=get))

    def test_birth_manifest_and_stopped_fixture_are_not_mutated_or_adopted_as_staff(self):
        snapshot = self.inventory.snapshot()
        self.assertEqual(snapshot["baseline"], self.baseline)
        self.assertEqual(len(snapshot["slots"]), 4)
        self.assertEqual({row["role"] for row in snapshot["slots"]}, {"gateway", "collector"})
        self.assertNotIn(self.project + "-collector", {row["name"] for row in snapshot["slots"]})
        self.assertFalse(any("active" in row or "identity_verified" in row for row in snapshot["slots"]))
        self.assertEqual((self.root / MANIFEST).read_bytes(), self.manifest)

    def test_intent_nonce_and_unresolved_slot_survive_reopen_without_replay(self):
        self.assertTrue(self.inventory.reserve_creation("create-four", "collector", 4, 0))
        before = self.inventory.snapshot()
        self.reopen()
        self.assertFalse(self.inventory.reserve_creation("create-four", "collector", 4, 0))
        self.assertEqual(self.inventory.snapshot(), before)
        self.assertIsNone(before["creations"][0]["record"])
        self.assertIsNone(next(s for s in before["slots"] if s["slot"] == 4)["record"])
        self.assertEqual((self.root / MANIFEST).read_bytes(), self.manifest)

    def test_stale_revision_collision_occupied_slot_and_capacity_bounds_fail_closed(self):
        for role, slot in (("collector", 0), ("gateway", 5), ("controller", 1), ("collector", True)):
            with self.subTest(role=role, slot=slot), self.assertRaises(Denial):
                self.inventory.reserve_creation("invalid", role, slot, 0)
        with self.assertRaisesRegex(Denial, "conflict"):
            self.inventory.reserve_creation("occupied", "collector", 1, 0)
        self.inventory.reserve_creation("create-four", "collector", 4, 0)
        with self.assertRaisesRegex(Denial, "stale"):
            self.inventory.reserve_creation("gateway-two", "gateway", 2, 0)
        with self.assertRaisesRegex(Denial, "conflict"):
            self.inventory.reserve_creation("create-four", "gateway", 2, 1)
        with self.assertRaisesRegex(Denial, "conflict"):
            self.inventory.reserve_creation("other-id", "collector", 4, 1)
        self.reopen(limit=1)
        self.assertFalse(self.inventory.reserve_creation("create-four", "collector", 4, 1))
        with self.assertRaisesRegex(Denial, "limit"):
            self.inventory.reserve_creation("gateway-two", "gateway", 2, 1)

    def test_concurrent_intents_only_one_wins_expected_revision(self):
        def reserve(operation, role, slot):
            try:
                return self.inventory.reserve_creation(operation, role, slot, 0)
            except Denial as error:
                return error.code
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda args: reserve(*args),
                                        (("collector-four", "collector", 4), ("gateway-two", "gateway", 2))))
        self.assertCountEqual(results, [True, "stale"])
        self.assertEqual(self.inventory.snapshot()["revision"], 1)

    def test_failed_persistence_rolls_back_revision_and_intent_atomically(self):
        self.inventory.connection.execute(
            "CREATE TRIGGER synthetic_failure BEFORE INSERT ON fleet_creations "
            "BEGIN SELECT RAISE(ABORT,'synthetic interrupted persist'); END")
        before = self.inventory.snapshot()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "interrupted persist"):
            self.inventory.reserve_creation("create-four", "collector", 4, 0)
        self.assertEqual(self.inventory.snapshot(), before)
        self.inventory.connection.execute("DROP TRIGGER synthetic_failure")
        self.assertTrue(self.inventory.reserve_creation("create-four", "collector", 4, 0))

    def test_exact_nonce_labels_start_and_config_image_bind_after_lost_response(self):
        self.inventory.reserve_creation("create-four", "collector", 4, 0)
        self.reopen()
        _, engine = self.candidate()
        bound = self.inventory.bind_creation("create-four", engine, 1)
        self.assertEqual(self.inventory.snapshot()["revision"], 2)
        query = engine.get.call_args_list[0].args[0]
        self.assertIn("limit=2", query)
        self.assertIn("bridge-creation-nonce", query)
        self.reopen()
        self.assertEqual(self.inventory.bind_creation("create-four", engine, 1), bound)
        self.assertEqual(self.inventory.snapshot()["revision"], 2)
        self.assertEqual((self.root / MANIFEST).read_bytes(), self.manifest)

    def test_missing_ambiguous_or_foreign_creation_never_binds_or_retries_creation(self):
        self.inventory.reserve_creation("create-four", "collector", 4, 0)
        for candidates, error in (([], RuntimeError), ([{"Id": "9" * 64}] * 2, Denial)):
            with self.subTest(candidates=candidates), self.assertRaises(error):
                self.inventory.bind_creation("create-four", Mock(get=Mock(return_value=candidates)), 1)
        state, engine = self.candidate()
        original = copy.deepcopy(state)
        changes = [
            lambda s: s.update(Name="/same-name-other-container"),
            lambda s: s.update(Image="sha256:" + "b" * 64),
            lambda s: s["Config"]["Labels"].update({"delta-runtime": "unrelated"}),
            lambda s: s["Config"]["Labels"].update({"bridge-creation-nonce": "other-nonce"}),
            lambda s: s["Config"]["Labels"].update({"bridge-deployment-incarnation": "0" * 32}),
            lambda s: s["State"].update(Running=False),
            lambda s: s["State"].update(Paused=True),
            lambda s: s["State"].pop("Paused"),
        ]
        for change in changes:
            state.clear()
            state.update(copy.deepcopy(original))
            change(state)
            with self.subTest(change=change), self.assertRaisesRegex(Denial, "wrong-target"):
                self.inventory.bind_creation("create-four", engine, 1)
        self.assertIsNone(self.inventory.snapshot()["creations"][0]["record"])
        self.assertEqual(self.inventory.snapshot()["revision"], 1)

    def test_replacement_and_existing_baseline_id_never_adopted(self):
        self.inventory.reserve_creation("create-four", "collector", 4, 0)
        _, baseline_engine = self.candidate(identity=next(iter(self.baseline.values()))["id"])
        with self.assertRaisesRegex(Denial, "wrong-target"):
            self.inventory.bind_creation("create-four", baseline_engine, 1)
        state, engine = self.candidate()
        self.inventory.bind_creation("create-four", engine, 1)
        state["State"]["StartedAt"] = "replacement-start"
        with self.assertRaisesRegex(Denial, "wrong-target"):
            self.inventory.bind_creation("create-four", engine, 2)

    def test_fresh_revision_is_rechecked_after_read_only_probe(self):
        self.inventory.reserve_creation("create-four", "collector", 4, 0)
        _, engine = self.candidate()
        self.inventory.reserve_creation("gateway-two", "gateway", 2, 1)
        with self.assertRaisesRegex(Denial, "stale"):
            self.inventory.bind_creation("create-four", engine, 1)
        self.assertIsNone(self.inventory.snapshot()["creations"][0]["record"])
        self.assertEqual(self.inventory.bind_creation("create-four", engine, 2)["id"], "9" * 64)

    def test_exclusive_owner_private_database_and_persisted_tamper(self):
        with self.assertRaises(BlockingIOError):
            FleetInventory(self.root, self.deployment)
        self.assertEqual((self.root / "bridge-fleet.sqlite").stat().st_mode & 0o777, 0o600)
        self.inventory.reserve_creation("create-four", "collector", 4, 0)
        self.inventory.connection.execute("UPDATE fleet_creations SET nonce='tampered'")
        with self.assertRaisesRegex(ValueError, "persisted fleet creation"):
            self.reopen()

    def test_changed_manifest_or_database_symlink_never_attaches(self):
        self.inventory.close()
        self.inventory = None
        manifest = json.loads(self.manifest)
        manifest["deployment"]["incarnation"] = "0" * 32
        (self.root / MANIFEST).write_text(json.dumps(manifest))
        with self.assertRaisesRegex(Denial, "wrong-target"):
            FleetInventory(self.root, self.deployment)
        (self.root / MANIFEST).write_bytes(self.manifest)
        path = self.root / "bridge-fleet.sqlite"
        backup = self.root / "saved.sqlite"
        path.rename(backup)
        path.symlink_to(backup)
        with self.assertRaises(OSError):
            FleetInventory(self.root, self.deployment)

    def test_actual_child_exit_after_durable_intent_releases_lease_and_preserves_nonce(self):
        self.inventory.close()
        self.inventory = None
        program = (
            "import json,os,sys; from pathlib import Path; from live_inventory import FleetInventory; "
            "i=FleetInventory(Path(sys.argv[1]),json.loads(sys.argv[2])); "
            "i.reserve_creation('crashed-owner','collector',4,0); os._exit(37)"
        )
        environment = dict(os.environ)
        environment["PYTHONPATH"] = os.pathsep.join(sys.path)
        child = subprocess.run([sys.executable, "-B", "-c", program, str(self.root),
                                json.dumps(self.deployment)], env=environment, capture_output=True, timeout=10)
        self.assertEqual(child.returncode, 37, child.stderr.decode())
        self.inventory = FleetInventory(self.root, self.deployment)
        prior = self.inventory.snapshot()
        self.assertFalse(self.inventory.reserve_creation("crashed-owner", "collector", 4, 0))
        self.assertEqual(self.inventory.snapshot(), prior)
        self.assertIsNone(prior["creations"][0]["record"])


if __name__ == "__main__":
    unittest.main()
