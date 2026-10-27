import json
import unittest
import uuid
from unittest.mock import patch

from bootstrap import Failure
from publisher_control import FENCED, READY, PublisherControl, UnknownTransition
from test_bootstrap import FakeAzure


class Authority(FakeAzure):
    def __init__(self):
        super().__init__()
        self.metadata.update(resetstate=READY, publisherregistryversion="1",
                             publishers="{}", custom="preserved")

    def blob(self, method, container, **kwargs):
        if method != "HEAD":
            raise AssertionError("Only metadata reads are expected here")
        return b"", {**{"x-ms-meta-" + key: value for key, value in self.metadata.items()},
                     "etag": self.etag}


class PublisherControlTests(unittest.TestCase):
    def setUp(self):
        self.azure = Authority()
        self.control = PublisherControl(self.azure)
        self.generation = self.azure.metadata["resetgeneration"]
        self.publisher = str(uuid.uuid4())

    def registry(self):
        return json.loads(self.azure.metadata["publishers"])

    def test_normal_admission_and_explicit_definitive_drain_preserve_all_metadata(self):
        self.control.admit(self.publisher, self.generation)
        self.assertEqual(self.registry()[self.publisher]["state"], "active")
        self.control.clean_release(self.publisher, self.generation, definitively_drained=True)
        self.assertEqual(self.registry(), {})
        self.assertEqual(self.azure.metadata["custom"], "preserved")

    def test_fence_preserves_generation_members_and_allows_existing_clean_drain(self):
        self.control.admit(self.publisher, self.generation)
        self.control.fence(self.generation)
        self.assertEqual(self.azure.metadata["resetgeneration"], self.generation)
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)
        with self.assertRaises(Failure):
            self.control.admit(str(uuid.uuid4()), self.generation)
        with self.assertRaises(Failure):
            self.control.drained(self.generation)
        self.control.clean_release(self.publisher, self.generation, definitively_drained=True)
        self.control.drained(self.generation)

    def test_uncertain_member_cannot_be_released_even_after_logical_commit_reconciliation(self):
        self.control.admit(self.publisher, self.generation)
        self.control.uncertain(self.publisher, self.generation)
        self.control.fence(self.generation)
        with self.assertRaises(Failure):
            self.control.clean_release(self.publisher, self.generation, definitively_drained=True)
        with self.assertRaises(Failure):
            self.control.rotate_drained(self.generation, str(uuid.uuid4()))
        self.assertEqual(self.registry()[self.publisher]["state"], "uncertain")

    def test_suspended_or_crashed_registration_does_not_expire(self):
        self.control.admit(self.publisher, self.generation)
        self.control.fence(self.generation)
        for _ in range(3):
            with self.assertRaises(Failure):
                self.control.drained(self.generation)
        self.assertIn(self.publisher, self.registry())

    def test_exclusive_bootstrap_releases_and_sets_ready_in_single_cas(self):
        self.control.fence(self.generation)
        fresh = str(uuid.uuid4())
        self.control.rotate_drained(self.generation, fresh)
        self.control.admit(self.publisher, fresh, bootstrap=True)
        with self.assertRaises(Failure):
            self.control.admit(str(uuid.uuid4()), fresh, bootstrap=True)
        before = len(self.azure.events)
        self.control.complete_bootstrap(self.publisher, fresh, definitively_drained=True)
        self.assertEqual(len(self.azure.events), before + 1)
        self.assertEqual(self.azure.metadata["resetstate"], READY)
        self.assertEqual(self.registry(), {})

    def test_bootstrap_error_leaves_blocking_operator_and_no_ready_transition(self):
        self.control.fence(self.generation)
        self.control.admit(self.publisher, self.generation, bootstrap=True)
        with self.assertRaises(Failure):
            self.control.complete_bootstrap(self.publisher, self.generation, definitively_drained=False)
        self.assertIn(self.publisher, self.registry())
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)

    def test_unknown_cas_response_never_retries_or_admits_success(self):
        original = self.azure.set_control
        def applied_then_lost(metadata, etag):
            original(metadata, etag)
            raise Failure("response lost")
        with patch.object(self.azure, "set_control", side_effect=applied_then_lost) as cas:
            with self.assertRaises(UnknownTransition):
                self.control.admit(self.publisher, self.generation)
            self.assertEqual(cas.call_count, 1)
        self.assertIn(self.publisher, self.registry())
        self.control.fence(self.generation)
        with self.assertRaises(Failure):
            self.control.drained(self.generation)

    def test_admission_fence_etag_race_is_fail_closed(self):
        original = self.azure.set_control
        def concurrent_fence(metadata, etag):
            self.azure.metadata["resetstate"] = FENCED
            self.azure.etag = '"concurrent-fence"'
            original(metadata, etag)
        with patch.object(self.azure, "set_control", side_effect=concurrent_fence):
            with self.assertRaises(UnknownTransition):
                self.control.admit(self.publisher, self.generation)
        self.assertEqual(self.registry(), {})
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)

    def test_capacity_blocks_seventeenth_publisher(self):
        for _ in range(16):
            self.control.admit(str(uuid.uuid4()), self.generation)
        with self.assertRaises(Failure):
            self.control.admit(str(uuid.uuid4()), self.generation)
        self.assertEqual(len(self.registry()), 16)

    def test_total_metadata_budget_fails_closed(self):
        self.azure.metadata["custom"] = "x" * 8000
        with self.assertRaises(Failure):
            self.control.admit(self.publisher, self.generation)
        self.assertEqual(self.registry(), {})

    def test_legacy_state_missing_registry_and_foreign_generation_fail_closed(self):
        for field, value in (("resetstate", "ready"), ("publisherregistryversion", "2"),
                             ("publishers", '{"bad":{}}')):
            with self.subTest(field=field):
                azure = Authority()
                azure.metadata[field] = value
                with self.assertRaises(Failure):
                    PublisherControl(azure).read()
        self.azure.metadata["publishers"] = json.dumps({
            self.publisher: {"generation": str(uuid.uuid4()), "state": "active"},
        })
        with self.assertRaises(Failure):
            self.control.read()

    def test_repeat_identity_never_adopts_an_unknown_prior_admission(self):
        self.control.admit(self.publisher, self.generation)
        with self.assertRaises(Failure):
            self.control.admit(self.publisher, self.generation)
        self.assertEqual(len(self.registry()), 1)

    def test_occupancy_blocks_fence_and_bootstrap(self):
        self.azure.metadata["occupancy"] = "testing"
        with self.assertRaises(Failure):
            self.control.fence(self.generation)
        self.azure.metadata["resetstate"] = FENCED
        with self.assertRaises(Failure):
            self.control.admit(self.publisher, self.generation, bootstrap=True)

    def test_generation_rotation_requires_empty_same_generation_fenced_registry(self):
        with self.assertRaises(Failure):
            self.control.rotate_drained(self.generation, str(uuid.uuid4()))
        self.control.fence(self.generation)
        with self.assertRaises(Failure):
            self.control.rotate_drained(str(uuid.uuid4()), str(uuid.uuid4()))
        with self.assertRaises(Failure):
            self.control.rotate_drained(self.generation, self.generation)

    def test_cas_unknown_on_bootstrap_completion_does_not_report_success(self):
        self.control.fence(self.generation)
        self.control.admit(self.publisher, self.generation, bootstrap=True)
        with patch.object(self.azure, "set_control", side_effect=Failure("timeout")):
            with self.assertRaises(UnknownTransition):
                self.control.complete_bootstrap(self.publisher, self.generation, definitively_drained=True)
        self.assertIn(self.publisher, self.registry())
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)


if __name__ == "__main__":
    unittest.main()
