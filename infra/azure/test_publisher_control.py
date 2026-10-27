import json
import unittest
import uuid
from unittest.mock import patch

from bootstrap import Failure
from publisher_control import CLEANING, FENCED, READY, PublisherControl, UnknownTransition
from test_bootstrap import FakeAzure


class Authority(FakeAzure):
    def __init__(self):
        super().__init__()
        self.metadata.update(resetstate=READY, publisherregistryversion="1",
                             publishers="{}", occupancy="released", custom="preserved")


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
            self.control.claim_cleanup(str(uuid.uuid4()), self.generation)
        self.assertEqual(self.registry()[self.publisher]["state"], "uncertain")

    def test_suspended_or_crashed_registration_does_not_expire(self):
        self.control.admit(self.publisher, self.generation)
        self.control.fence(self.generation)
        for _ in range(3):
            with self.assertRaises(Failure):
                self.control.drained(self.generation)
        self.assertIn(self.publisher, self.registry())

    def child_bootstrap(self, generation, child=None, *, drain=True):
        """Model of the Rust child: own fresh UUID, never supplied by the parent."""
        child = child or str(uuid.uuid4())
        self.control.admit(child, generation, bootstrap=True)
        if drain:
            self.control.clean_release(child, generation, definitively_drained=True)
        return child

    def parent_admit(self, generation):
        parent = str(uuid.uuid4())
        self.control.admit(parent, generation, bootstrap=True)
        return parent

    def test_child_then_fresh_parent_member_completes_ready_in_single_cas(self):
        self.control.fence(self.generation)
        fresh, cleaner = str(uuid.uuid4()), str(uuid.uuid4())
        self.control.claim_cleanup(cleaner, self.generation)
        self.control.finish_cleanup(cleaner, self.generation, fresh, definitively_cleaned=True)
        child = self.child_bootstrap(fresh, drain=False)
        with self.assertRaises(Failure):  # parent cannot admit while child holds
            self.parent_admit(fresh)
        self.control.clean_release(child, fresh, definitively_drained=True)
        parent = self.parent_admit(fresh)
        self.assertNotEqual(parent, child)
        with self.assertRaises(Failure):  # cleanup cannot claim during parent KQL
            self.control.claim_cleanup(cleaner, fresh)
        self.control.bootstrap_owner(parent, fresh)
        with self.assertRaises(Failure):  # child UUID is never adopted
            self.control.complete_bootstrap(child, fresh, definitively_drained=True)
        before = len(self.azure.events)
        self.control.complete_bootstrap(parent, fresh, definitively_drained=True)
        self.assertEqual(len(self.azure.events), before + 1)
        self.assertEqual(self.azure.metadata["resetstate"], READY)
        self.assertEqual(self.registry(), {})

    def test_cleanup_wins_after_child_drain_parent_admission_fails_before_io(self):
        self.control.fence(self.generation)
        self.child_bootstrap(self.generation)
        cleaner = str(uuid.uuid4())
        self.control.claim_cleanup(cleaner, self.generation)
        with self.assertRaises(Failure):  # parent must stop before any KQL
            self.parent_admit(self.generation)
        self.control.finish_cleanup(cleaner, self.generation, str(uuid.uuid4()),
                                    definitively_cleaned=True)
        with self.assertRaises(Failure):  # stale captured generation
            self.parent_admit(self.generation)
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)
        self.assertEqual(self.registry(), {})

    def test_parent_wins_admission_then_cleanup_claim_fails(self):
        self.control.fence(self.generation)
        self.child_bootstrap(self.generation)
        parent = self.parent_admit(self.generation)
        with self.assertRaises(Failure):
            self.control.claim_cleanup(str(uuid.uuid4()), self.generation)
        self.control.bootstrap_owner(parent, self.generation)

    def test_unknown_parent_admin_outcome_retains_member_and_blocks_ready(self):
        self.control.fence(self.generation)
        self.child_bootstrap(self.generation)
        parent = self.parent_admit(self.generation)
        self.control.uncertain(parent, self.generation)
        for drained in (True, False):
            with self.assertRaises(Failure):
                self.control.complete_bootstrap(parent, self.generation,
                                                definitively_drained=drained)
        with self.assertRaises(Failure):
            self.control.claim_cleanup(str(uuid.uuid4()), self.generation)
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)

    def test_uncertain_child_blocks_parent_admission(self):
        self.control.fence(self.generation)
        child = self.child_bootstrap(self.generation, drain=False)
        self.control.uncertain(child, self.generation)
        with self.assertRaises(Failure):
            self.control.clean_release(child, self.generation, definitively_drained=True)
        with self.assertRaises(Failure):
            self.parent_admit(self.generation)
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
                             ("occupancy", None), ("occupancy", "stopped"),
                             ("publishers", '{"bad":{}}')):
            with self.subTest(field=field):
                azure = Authority()
                if value is None:
                    del azure.metadata[field]
                else:
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
        self.azure.metadata.update(occupancy="testing", occupancyid=str(uuid.uuid4()))
        with self.assertRaises(Failure):
            self.control.fence(self.generation)
        self.azure.metadata["resetstate"] = FENCED
        with self.assertRaises(Failure):
            self.control.admit(self.publisher, self.generation, bootstrap=True)

    def test_generation_rotation_requires_held_cleanup_and_fresh_uuid(self):
        cleaner = str(uuid.uuid4())
        with self.assertRaises(Failure):
            self.control.claim_cleanup(cleaner, self.generation)
        self.control.fence(self.generation)
        with self.assertRaises(Failure):
            self.control.claim_cleanup(cleaner, str(uuid.uuid4()))
        self.control.claim_cleanup(cleaner, self.generation)
        with self.assertRaises(Failure):
            self.control.finish_cleanup(cleaner, self.generation, self.generation,
                                        definitively_cleaned=True)
        with self.assertRaises(Failure):
            self.control.finish_cleanup(cleaner, self.generation, str(uuid.uuid4()),
                                        definitively_cleaned=False)
        with self.assertRaises(Failure):
            self.control.finish_cleanup(str(uuid.uuid4()), self.generation, str(uuid.uuid4()),
                                        definitively_cleaned=True)
        self.assertEqual(self.azure.metadata["resetstate"], CLEANING)

    def test_drain_snapshot_then_bootstrap_admission_defeats_later_cleanup_claim(self):
        cleaner = str(uuid.uuid4())
        self.control.fence(self.generation)
        self.control.drained(self.generation)  # snapshot only, not authority
        self.control.admit(self.publisher, self.generation, bootstrap=True)
        with self.assertRaises(Failure):
            self.control.claim_cleanup(cleaner, self.generation)
        with self.assertRaises(Failure):
            self.control.cleanup_operator(cleaner, self.generation)
        self.assertEqual(set(self.registry()), {self.publisher})
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)

    def test_cleanup_claim_blocks_bootstrap_admission_through_all_deletion(self):
        cleaner = str(uuid.uuid4())
        self.control.fence(self.generation)
        self.control.claim_cleanup(cleaner, self.generation)
        for _ in range(3):  # each deletion step re-verifies held authority
            self.control.cleanup_operator(cleaner, self.generation)
            for attempt in (lambda: self.control.admit(str(uuid.uuid4()), self.generation, bootstrap=True),
                            lambda: self.control.admit(str(uuid.uuid4()), self.generation),
                            lambda: self.control.claim_cleanup(str(uuid.uuid4()), self.generation),
                            lambda: self.control.fence(self.generation),
                            lambda: self.control.complete_bootstrap(cleaner, self.generation, definitively_drained=True),
                            lambda: self.control.clean_release(cleaner, self.generation,
                                                               definitively_drained=True)):
                with self.assertRaises(Failure):
                    attempt()
        self.assertEqual(set(self.registry()), {cleaner})
        self.assertEqual(self.azure.metadata["resetstate"], CLEANING)

    def test_concurrent_bootstrap_and_cleanup_claims_cannot_both_win(self):
        cleaner = str(uuid.uuid4())
        self.control.fence(self.generation)
        original = self.azure.set_control
        competitor = PublisherControl(self.azure)
        def bootstrap_wins_first(metadata, etag):
            with patch.object(self.azure, "set_control", side_effect=original):
                competitor.admit(self.publisher, self.generation, bootstrap=True)
            original(metadata, etag)
        with patch.object(self.azure, "set_control", side_effect=bootstrap_wins_first):
            with self.assertRaises(UnknownTransition):
                self.control.claim_cleanup(cleaner, self.generation)
        self.assertEqual(set(self.registry()), {self.publisher})
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)
        with self.assertRaises(Failure):
            self.control.cleanup_operator(cleaner, self.generation)

    def test_concurrent_cleanup_claim_defeats_bootstrap_admission(self):
        cleaner = str(uuid.uuid4())
        self.control.fence(self.generation)
        original = self.azure.set_control
        competitor = PublisherControl(self.azure)
        def cleanup_wins_first(metadata, etag):
            with patch.object(self.azure, "set_control", side_effect=original):
                competitor.claim_cleanup(cleaner, self.generation)
            original(metadata, etag)
        with patch.object(self.azure, "set_control", side_effect=cleanup_wins_first):
            with self.assertRaises(UnknownTransition):
                self.control.admit(self.publisher, self.generation, bootstrap=True)
        self.assertEqual(set(self.registry()), {cleaner})
        self.assertEqual(self.azure.metadata["resetstate"], CLEANING)

    def test_finish_cleanup_then_bootstrap_then_cleanup_ordering(self):
        cleaner, fresh = str(uuid.uuid4()), str(uuid.uuid4())
        self.control.fence(self.generation)
        self.control.claim_cleanup(cleaner, self.generation)
        with self.assertRaises(Failure):  # old generation cannot bootstrap during cleanup
            self.control.admit(self.publisher, self.generation, bootstrap=True)
        self.control.finish_cleanup(cleaner, self.generation, fresh, definitively_cleaned=True)
        self.assertEqual((self.azure.metadata["resetstate"], self.registry()), (FENCED, {}))
        with self.assertRaises(Failure):  # stale generation admission rejected
            self.control.admit(self.publisher, self.generation, bootstrap=True)
        self.control.admit(self.publisher, fresh, bootstrap=True)
        with self.assertRaises(Failure):
            self.control.claim_cleanup(str(uuid.uuid4()), fresh)
        with self.assertRaises(Failure):
            self.control.cleanup_operator(cleaner, fresh)

    def test_unknown_cleanup_claim_is_never_adopted_and_blocks_bootstrap(self):
        cleaner = str(uuid.uuid4())
        self.control.fence(self.generation)
        original = self.azure.set_control
        def applied_then_lost(metadata, etag):
            original(metadata, etag)
            raise Failure("response lost")
        with patch.object(self.azure, "set_control", side_effect=applied_then_lost):
            with self.assertRaises(UnknownTransition):
                self.control.claim_cleanup(cleaner, self.generation)
        with self.assertRaises(Failure):
            self.control.claim_cleanup(cleaner, self.generation)
        with self.assertRaises(Failure):
            self.control.admit(self.publisher, self.generation, bootstrap=True)
        self.assertEqual(self.azure.metadata["resetstate"], CLEANING)

    def test_unknown_finish_cleanup_does_not_report_bootstrap_eligibility(self):
        cleaner = str(uuid.uuid4())
        self.control.fence(self.generation)
        self.control.claim_cleanup(cleaner, self.generation)
        with patch.object(self.azure, "set_control", side_effect=Failure("timeout")):
            with self.assertRaises(UnknownTransition):
                self.control.finish_cleanup(cleaner, self.generation, str(uuid.uuid4()),
                                            definitively_cleaned=True)
        self.assertEqual(self.azure.metadata["resetstate"], CLEANING)
        self.assertEqual(set(self.registry()), {cleaner})

    def test_uncertain_cleanup_operator_never_releases(self):
        cleaner = str(uuid.uuid4())
        self.control.fence(self.generation)
        self.control.claim_cleanup(cleaner, self.generation)
        self.control.uncertain(cleaner, self.generation)
        with self.assertRaises(Failure):
            self.control.finish_cleanup(cleaner, self.generation, str(uuid.uuid4()),
                                        definitively_cleaned=True)
        self.assertEqual(self.registry()[cleaner]["state"], "uncertain")
    def test_cas_unknown_on_parent_completion_does_not_report_success(self):
        self.control.fence(self.generation)
        self.child_bootstrap(self.generation)
        parent = self.parent_admit(self.generation)
        with patch.object(self.azure, "set_control", side_effect=Failure("timeout")):
            with self.assertRaises(UnknownTransition):
                self.control.complete_bootstrap(parent, self.generation,
                                                definitively_drained=True)
        self.assertEqual(self.azure.metadata["resetstate"], FENCED)

if __name__ == "__main__":
    unittest.main()
