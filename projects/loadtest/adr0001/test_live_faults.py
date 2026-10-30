"""Synthetic supervisor fixtures; these do not qualify real Docker effects."""
import copy
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from live_contract import Denial
from live_faults import FaultJournal, FaultSupervisor, boot_identity, supervise


class FaultSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.root.chmod(0o700)
        self.project = "publication-gate-synthetic-faults"
        self.identity = "a" * 64
        self.name = self.project + "-collector1"
        self.state = {
            "Id": self.identity, "Name": "/" + self.name, "Image": "sha256:" + "b" * 64,
            "Config": {"Labels": {"delta-runtime": self.project}},
            "State": {"StartedAt": "synthetic-start", "Running": True, "Paused": False},
        }
        self.runtime = SimpleNamespace(
            root=self.root, project=self.project, subnet="10.200.1.0/24", arm="adr",
            deployment={"id": self.project, "incarnation": "c" * 32}, names={"collector1": self.name},
            sql_name=self.project + "-sql", owned_container=lambda _: self.identity,
            engine=SimpleNamespace(get=lambda _: copy.deepcopy(self.state)),
        )
        self.supervisor = FaultSupervisor(self.runtime)
        self.journal = self.supervisor.journal
        self.action = {
            "operation": "fault", "resource": {
                "id": "resource-collector1",
                "incarnation": hashlib.sha256((self.identity + "synthetic-start").encode()).hexdigest(),
            }, "fault": "pause", "duration_ms": 1,
        }
        self.calls = []

    def tearDown(self):
        self.journal.close()
        self.directory.cleanup()

    def reserve(self, operation_id="synthetic-fault", action=None):
        action = action or self.action
        resource = {"id": self.identity, "started_at": "synthetic-start",
                    "image": self.state["Image"], "name": self.name}
        return self.journal.reserve(
            operation_id, {"deployment": self.runtime.deployment, "action": action}, resource)

    def effect(self, operation, identity):
        self.assertEqual(identity, self.identity)
        self.calls.append(operation)
        self.state["State"]["Paused"] = operation == "pause"

    def run_worker(self, operation_id="synthetic-fault", repair_only=False, effect=None):
        return supervise(self.runtime, self.journal, operation_id, repair_only, effect or self.effect)

    def test_bounded_fault_and_exact_rollback_are_durable_and_not_replayed(self):
        self.reserve()
        result = self.run_worker()
        self.assertEqual(result["phase"], "repaired")
        self.assertTrue(result["applied"])
        self.assertEqual(self.calls, ["pause", "unpause"])
        self.run_worker()
        self.assertEqual(self.calls, ["pause", "unpause"])
        reopened = FaultJournal(self.root)
        try:
            self.assertEqual(reopened.get("synthetic-fault"), result)
            self.assertEqual(reopened.unresolved(), [])
        finally:
            reopened.close()
        self.assertEqual(os.stat(self.root / "bridge-faults.sqlite").st_mode & 0o777, 0o600)

    def test_same_id_has_one_reservation_and_conflicting_or_concurrent_work_is_denied(self):
        first, fresh = self.reserve()
        self.assertTrue(fresh)
        self.assertEqual(self.reserve(), (first, False))
        with self.assertRaises(Denial):
            self.reserve(action={**self.action, "duration_ms": 2})
        with self.assertRaises(Denial):
            self.reserve("another-fault")

    def test_pending_recovery_never_introduces_a_new_pause(self):
        self.reserve()
        result = self.run_worker(repair_only=True)
        self.assertEqual(result["phase"], "rejected")
        self.assertFalse(result["applied"])
        self.assertEqual(self.calls, [])

    def test_uncertain_pause_and_changed_boot_only_compensate(self):
        self.reserve()
        self.journal.update("synthetic-fault", phase="pause-sent", deadline_ns=1)
        self.state["State"]["Paused"] = True
        with patch("live_faults.boot_identity", return_value="different-boot"):
            result = self.run_worker()
        self.assertEqual(self.calls, ["unpause"])
        self.assertEqual(result["phase"], "repaired")
        self.assertFalse(result["applied"])

    def test_preexisting_pause_and_replaced_incarnation_are_never_touched(self):
        self.reserve()
        self.state["State"]["Paused"] = True
        self.assertEqual(self.run_worker()["phase"], "rejected")
        self.assertEqual(self.calls, [])
        self.state["State"]["Paused"] = False
        self.reserve("replacement")
        self.journal.update("replacement", phase="pause-sent")
        self.state["State"]["StartedAt"] = "new-process"
        self.assertEqual(self.run_worker("replacement", repair_only=True)["phase"], "repair-required")
        self.assertEqual(self.calls, [])

    def test_lost_pause_response_is_compensated_without_inventing_application(self):
        self.reserve()
        def lose_response(operation, identity):
            self.effect(operation, identity)
            if operation == "pause":
                raise subprocess.TimeoutExpired(["synthetic"], 2)
        result = self.run_worker(effect=lose_response)
        self.assertEqual(self.calls, ["pause", "unpause"])
        self.assertEqual(result["phase"], "repaired")
        self.assertFalse(result["applied"])
        self.assertEqual(result["error_code"], "unknown")

    def test_failed_rollback_remains_unresolved_and_retry_only_unpauses(self):
        self.reserve()
        def failed_rollback(operation, identity):
            if operation == "unpause":
                raise RuntimeError("synthetic engine unavailable")
            self.effect(operation, identity)
        self.assertEqual(self.run_worker(effect=failed_rollback)["phase"], "repair-required")
        self.assertEqual(len(self.journal.unresolved()), 1)
        result = self.run_worker(repair_only=True)
        self.assertEqual(result["phase"], "repaired")
        self.assertEqual(self.calls, ["pause", "unpause"])

    def test_expired_or_replaced_boot_reservation_never_pauses(self):
        self.reserve()
        with patch("live_faults.boot_identity", return_value="different-boot"):
            self.assertEqual(self.run_worker()["phase"], "rejected")
        self.assertEqual(self.calls, [])
        self.assertEqual(len(boot_identity()), 64)

    def test_launch_is_closed_bounded_and_starts_one_independent_worker(self):
        writes = []
        child = SimpleNamespace(
            wait=lambda **_: 0,
            stdin=SimpleNamespace(write=writes.append, flush=lambda: None, close=lambda: None))
        with patch("live_faults.subprocess.Popen", return_value=child) as launch, \
                patch("live_faults.select.select", return_value=([1], [], [])), \
                patch("live_faults.os.read", return_value=b"R"):
            self.supervisor.launch("synthetic-fault", self.action, lambda: None)
            self.supervisor.launch("synthetic-fault", self.action, lambda: None)
        self.assertEqual(launch.call_count, 1)
        self.assertEqual(writes, [b"G"])
        self.assertTrue(launch.call_args.kwargs["start_new_session"])
        self.assertNotIn("shell", launch.call_args.kwargs)
        self.assertEqual(list(self.root.glob("fault-worker-*.log"))[0].stat().st_mode & 0o777, 0o600)
        for change in ({"duration_ms": 3001}, {"fault": "shell"}, {"shell": "untrusted"},
                       {"resource": {**self.action["resource"], "incarnation": "f" * 64}}):
            with self.subTest(change=change), self.assertRaises(Denial):
                self.supervisor.launch("invalid-fault", {**self.action, **change}, lambda: None)

    def test_final_disarm_or_dead_parent_handshake_prevents_pause(self):
        self.reserve()
        def disarmed():
            raise Denial("disarmed")
        result = supervise(self.runtime, self.journal, "synthetic-fault",
                           effect=self.effect, authorize=disarmed)
        self.assertEqual(result["phase"], "rejected")
        self.assertEqual(result["error_code"], "disarmed")
        self.assertEqual(self.calls, [])
        writes = []
        child = SimpleNamespace(stdin=SimpleNamespace(
            write=writes.append, flush=lambda: None, close=lambda: None))
        with patch("live_faults.subprocess.Popen", return_value=child), \
                patch("live_faults.select.select", return_value=([1], [], [])), \
                patch("live_faults.os.read", return_value=b"R"):
            with self.assertRaises(Denial):
                self.supervisor.launch("denied-fault", self.action, disarmed)
        self.assertEqual(writes, [])

    def test_journal_and_worker_lease_refuse_symlinks_or_simultaneous_owners(self):
        import fcntl
        self.reserve()
        descriptor = os.open(self.root / "bridge-fault-owner.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                self.run_worker()
        finally:
            os.close(descriptor)
        other = self.root / "other"
        other.mkdir(mode=0o700)
        (other / "bridge-faults.sqlite").symlink_to(self.root / "bridge-faults.sqlite")
        with self.assertRaises(OSError):
            FaultJournal(other)


if __name__ == "__main__":
    unittest.main()
