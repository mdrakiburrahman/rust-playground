"""Synthetic ownership fixtures, not real process-restart qualification."""
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import live_deployment
from live_deployment import AttachedRuntime, DeploymentLease, MANIFEST, save_deployment


class DeploymentAttachmentTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.repo = Path(self.directory.name)
        self.project = "publication-gate-synthetic-restart"
        self.root = self.repo / "target/publication-gate" / self.project
        self.root.mkdir(parents=True)
        self.root.chmod(0o700)
        (self.root / "bin").mkdir()
        (self.root / "bin/adr-flight").write_bytes(b"synthetic binary")
        (self.repo / "target/debug").mkdir()
        (self.repo / "target/debug/adr-flight").write_bytes(b"synthetic binary")
        (self.root / "config").mkdir()
        (self.root / "config/controller.json").write_text(json.dumps({"operator_token": "private-synthetic-token"}))
        names = {role: self.project + "-" + role for role in ("controller", "sql", "collector")}
        self.records = {
            str(index) * 64: {
                "Id": str(index) * 64, "Name": "/" + name, "Image": "sha256:" + "a" * 64,
                "Config": {"Labels": {"delta-runtime": self.project}},
                "State": {"StartedAt": "synthetic-start", "Running": role != "collector"},
            } for index, (role, name) in enumerate(names.items(), 1)
        }
        ids = {record["Name"][1:]: identity for identity, record in self.records.items()}
        self.runtime = SimpleNamespace(
            root=self.root, project=self.project, subnet="10.200.1.0/24", arm="adr",
            names={"controller": names["controller"]}, container_ids=ids,
            control_role="controller", control_port=18001,
        )
        self.deployment = {"id": self.project, "incarnation": "e" * 32}
        self.engine = SimpleNamespace(get=lambda path: copy.deepcopy(self.records[path.split("/")[2]]))
        self.patcher = patch.object(live_deployment, "REPO", self.repo)
        self.patcher.start()
        save_deployment(self.runtime, self.deployment, "f" * 64, self.engine)

    def tearDown(self):
        self.patcher.stop()
        self.directory.cleanup()

    def attach(self, proof="f" * 64):
        return AttachedRuntime(self.project, "10.200.1.0/24", "adr", proof, self.engine)

    def test_exact_attachment_preserves_identity_without_creating_containers(self):
        with patch("live_deployment.subprocess.run") as run:
            attached = self.attach()
        run.assert_not_called()
        self.assertEqual(attached.deployment, self.deployment)
        self.assertEqual(attached.container_ids, self.runtime.container_ids)
        self.assertEqual(self.attach(None).deployment, self.deployment)
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.attach("0" * 64)

    def test_exclusive_process_lease_releases_after_owner_exits(self):
        lease = DeploymentLease(self.root)
        try:
            with self.assertRaises(BlockingIOError):
                DeploymentLease(self.root)
        finally:
            lease.close()
        DeploymentLease(self.root).close()

    def test_manifest_is_complete_no_overwrite_and_contains_no_credentials(self):
        original = (self.root / MANIFEST).read_bytes()
        self.assertNotIn(b"private-synthetic-token", original)
        with self.assertRaises(FileExistsError):
            save_deployment(self.runtime, self.deployment, "f" * 64, self.engine)
        self.assertEqual((self.root / MANIFEST).read_bytes(), original)
        self.assertEqual(list(self.root.glob("." + MANIFEST + "-*")), [])

    def test_replacement_or_foreign_container_never_attaches(self):
        record = next(iter(self.records.values()))
        original = copy.deepcopy(record)
        for field in ("StartedAt", "owner"):
            record.clear()
            record.update(copy.deepcopy(original))
            if field == "StartedAt":
                record["State"]["StartedAt"] = "replacement-start"
            else:
                record["Config"]["Labels"] = {"delta-runtime": "foreign"}
            with self.subTest(field=field), self.assertRaises(ValueError):
                self.attach()

    def test_binary_or_symlink_tamper_fails_before_serving(self):
        binary = self.root / "bin/adr-flight"
        original = binary.read_bytes()
        binary.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "binary changed"):
            self.attach()
        binary.write_bytes(original)
        target = self.root / "saved-config.json"
        (self.root / "config/controller.json").rename(target)
        (self.root / "config/controller.json").symlink_to(target)
        with self.assertRaises(OSError):
            self.attach()

    def test_cleanup_stops_only_saved_exact_container_ids(self):
        attached = self.attach()
        def stop(command, **_):
            self.records[command[-1]]["State"]["Running"] = False
            return SimpleNamespace(returncode=0)
        with patch("live_deployment.subprocess.run", side_effect=stop) as run:
            attached.finish_runtime()
        self.assertEqual({call.args[0][-1] for call in run.call_args_list}, set(self.records))
        self.assertTrue(all(call.args[0][0:4] == ["docker", "stop", "-t", "1"] for call in run.call_args_list))
        self.assertTrue(all(not state["State"]["Running"] for state in self.records.values()))

    def test_changed_incarnation_is_not_stopped_by_cleanup(self):
        attached = self.attach()
        for state in self.records.values():
            state["State"]["StartedAt"] = "replacement-start"
        with patch("live_deployment.subprocess.run") as run:
            with self.assertRaisesRegex(ValueError, "changed deployment incarnation"):
                attached.finish_runtime()
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
