"""Exact-resource guards for the finite retirement qualification drivers."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from delta_runtime import Runtime
from publication_gate import Gate
import delta_benchmark


class FrozenBenchmarkTests(unittest.TestCase):
    def test_exact_frozen_source_and_manifest_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.rs"
            source.write_bytes(b"qualified source")
            manifest = root / "source-binary-sha256.json"
            manifest.write_text(json.dumps({"source.rs": hashlib.sha256(source.read_bytes()).hexdigest()}))
            revision = root / "revision.json"
            revision.write_text(json.dumps({
                "source_binary_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            }))
            with patch.object(delta_benchmark, "REPO", root):
                delta_benchmark.verify_frozen_sources(root)
                source.write_bytes(b"changed source")
                with self.assertRaisesRegex(AssertionError, "source/binary changed"):
                    delta_benchmark.verify_frozen_sources(root)
                manifest.write_text("{}")
                with self.assertRaisesRegex(AssertionError, "manifest changed"):
                    delta_benchmark.verify_frozen_sources(root)

    def test_calibration_is_bounded_and_full_nine_phase_cohort_is_preserved(self):
        self.assertEqual(len(delta_benchmark.PHASES), 9)
        self.assertEqual([phase[0] for phase in delta_benchmark.CALIBRATION_PHASES], ["cold", "warm"])
        self.assertEqual(sum(phase[1] for phase in delta_benchmark.CALIBRATION_PHASES), 6)

    def test_observer_failure_cannot_skip_owned_container_cleanup(self):
        benchmark = object.__new__(delta_benchmark.Benchmark)
        benchmark.observer = Mock()
        benchmark.observer.stop.side_effect = RuntimeError("retained observer failure")
        benchmark.names = {}
        benchmark.executor = Mock()
        benchmark.finish = Mock()
        with self.assertRaisesRegex(RuntimeError, "observer failure"):
            benchmark.finish_benchmark()
        benchmark.finish.assert_called_once_with()
        benchmark.executor.shutdown.assert_called_once_with(wait=True)


class ContainerOwnershipTests(unittest.TestCase):
    def setUp(self):
        self.gate = object.__new__(Gate)
        self.gate.project = "publication-gate-test"
        self.identity = "a" * 64
        self.name = self.gate.project + "-sql"
        self.gate.container_ids = {self.name: self.identity}
        self.gate.containers = [self.identity]

    def test_oversized_sql_dns_label_fails_before_deployment_creation(self):
        with self.assertRaisesRegex(AssertionError, "DNS label"):
            Gate("publication-gate-" + "a" * 50, "10.235.1.0/24")

    def test_control_commands_use_created_id_not_reusable_name(self):
        for action in ("stop", "start", "restart", "kill", "pause", "unpause"):
            with self.subTest(action=action):
                command = ["docker", action, self.name]
                self.assertEqual(self.gate.fence_container_command(command), ["docker", action, self.identity])
                self.assertEqual(command[-1], self.name)

    def test_unknown_names_and_ids_are_denied(self):
        for target in ("unrelated", "b" * 64):
            with self.subTest(target=target), self.assertRaises(AssertionError):
                self.gate.fence_container_command(["docker", "stop", target])

    def test_exec_with_stdin_is_bound_to_created_id(self):
        self.assertEqual(
            self.gate.fence_container_command(["docker", "exec", "-i", self.name, "sql-probe"]),
            ["docker", "exec", "-i", self.identity, "sql-probe"],
        )

    def test_creation_registers_exact_id_and_checks_owner(self):
        self.gate.container_ids.clear()
        self.gate.containers.clear()
        state = {"Id": self.identity, "Config": {"Labels": {"publication-gate": self.gate.project}}}
        self.gate.run = Mock(side_effect=[
            SimpleNamespace(stdout=self.identity + "\n"),
            SimpleNamespace(stdout=json.dumps([state])),
        ])
        self.gate.create_container([
            "docker", "run", "-d", "--name", self.name,
            "--label", f"publication-gate={self.gate.project}", "test-image",
        ])
        self.assertEqual(self.gate.container_ids, {self.name: self.identity})
        self.assertEqual(self.gate.containers, [self.identity])

    def test_foreign_identity_is_rejected_and_never_adopted_for_cleanup(self):
        self.gate.container_ids.clear()
        self.gate.containers.clear()
        state = {"Id": self.identity, "Config": {"Labels": {"publication-gate": "foreign"}}}
        self.gate.run = Mock(side_effect=[
            SimpleNamespace(stdout=self.identity),
            SimpleNamespace(stdout=json.dumps([state])),
        ])
        with self.assertRaises(AssertionError):
            self.gate.create_container([
                "docker", "run", "-d", "--name", self.name,
                "--label", f"publication-gate={self.gate.project}", "test-image",
            ])
        self.assertEqual(self.gate.containers, [])
        self.assertEqual(self.gate.container_ids, {})

    def test_existing_volume_is_never_reused_or_relabelled(self):
        self.gate.run = Mock(return_value=SimpleNamespace(stdout=self.name + "\n"))
        with self.assertRaises(AssertionError):
            self.gate.create_volume(self.name)
        self.gate.run.assert_called_once_with(["docker", "volume", "ls", "-q"])

    def test_new_volume_requires_exact_owner_label(self):
        self.gate.run = Mock(side_effect=[
            SimpleNamespace(stdout=""),
            SimpleNamespace(stdout=self.name),
            SimpleNamespace(stdout=json.dumps([{"Name": self.name, "Labels": {"publication-gate": "foreign"}}])),
        ])
        with self.assertRaises(AssertionError):
            self.gate.create_volume(self.name)


class RetirementResourceTests(unittest.TestCase):
    def setUp(self):
        self.runtime = object.__new__(Runtime)
        self.runtime.project = "test-owned-deployment"
        self.runtime.config = {"collectors": {"collector1": "test-only"}}
        self.runtime.names = {"collector1": "test-owned-collector1"}
        self.container = {
            "Id": "test-exact-container",
            "Config": {"Labels": {"delta-runtime": self.runtime.project}},
            "State": {"Running": True, "StartedAt": "test-start-1"},
        }
        self.runtime.container_ids = {self.runtime.names["collector1"]: self.container["Id"]}
        self.runtime.run = Mock(return_value=SimpleNamespace(stdout=json.dumps([self.container])))

    def test_owned_approved_resource_is_stopped_by_id_not_name(self):
        resource = self.runtime.collector_resource("collector1")
        self.runtime.stop_retired_resource("collector1", resource)
        self.runtime.run.assert_called_with(["docker", "stop", "-t", "1", self.container["Id"]])

    def test_replaced_container_is_not_stopped(self):
        resource = self.runtime.collector_resource("collector1")
        replacement = copy.deepcopy(self.container)
        replacement["Id"] = "test-replacement"
        self.runtime.run.reset_mock()
        self.runtime.run.return_value = SimpleNamespace(stdout=json.dumps([replacement]))
        with self.assertRaises(AssertionError):
            self.runtime.stop_retired_resource("collector1", resource)
        self.runtime.run.assert_called_once_with(["docker", "inspect", self.runtime.names["collector1"]])

    def test_restarted_same_container_is_not_stopped(self):
        resource = self.runtime.collector_resource("collector1")
        self.container["State"]["StartedAt"] = "test-start-2"
        self.runtime.run.reset_mock()
        self.runtime.run.return_value = SimpleNamespace(stdout=json.dumps([self.container]))
        with self.assertRaises(AssertionError):
            self.runtime.stop_retired_resource("collector1", resource)
        self.runtime.run.assert_called_once_with(["docker", "inspect", self.runtime.names["collector1"]])

    def test_foreign_deployment_is_rejected(self):
        self.container["Config"]["Labels"]["delta-runtime"] = "test-foreign-deployment"
        self.runtime.run.return_value = SimpleNamespace(stdout=json.dumps([self.container]))
        with self.assertRaises(AssertionError):
            self.runtime.collector_resource("collector1")

    def test_unregistered_target_is_rejected_before_engine_command(self):
        with self.assertRaises(AssertionError):
            self.runtime.collector_resource("unrelated")
        self.runtime.run.assert_not_called()

    def test_stopped_resource_is_rejected(self):
        self.container["State"]["Running"] = False
        self.runtime.run.return_value = SimpleNamespace(stdout=json.dumps([self.container]))
        with self.assertRaises(AssertionError):
            self.runtime.collector_resource("collector1")


if __name__ == "__main__":
    unittest.main()
