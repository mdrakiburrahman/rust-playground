import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

import bootstrap as b


SUB = "00000000-0000-0000-0000-000000000001"
REAL_AZURE = b.Azure


class FakeAzure:
    def __init__(self):
        self.c = b.Config({"AZURE_SUBSCRIPTION_ID": SUB})
        c = self.c
        self.tags = {**b.TAGS, "sandboxId": c.sandbox}
        self.databases = [{"name": c.database}]
        self.containers = [{"name": name, "properties": {"metadata": {"sandboxid": c.sandbox}}}
                           for name in (c.data, c.checkpoints)]
        self.settings = {}
        self.items = {c.data: [{"name": "table/data.parquet", "snapshot": None,
                                "version": None, "deleted": False}], c.checkpoints: []}
        self.directories = {c.data: [], c.checkpoints: []}
        self.metadata = {"sandboxid": c.sandbox, "resetstate": "ready-v1",
                         "resetgeneration": str(uuid.uuid4()), "occupancy": "released",
                         "publisherregistryversion": "1", "publishers": "{}"}
        self.cluster_state = "Running"
        self.checkpoint_exists = True
        self.etag = '"one"'
        self.events = []

    def arm(self, method, path, **kwargs):
        if path.endswith("/blobServices/default"):
            return {"properties": self.settings}
        if path.endswith("/containers/" + self.c.checkpoints):
            return {"properties": {"metadata": dict(self.metadata)}} if self.checkpoint_exists else None
        return {"tags": dict(self.tags), "properties": {
            "isHnsEnabled": True, "uri": "https://cluster.example", "state": self.cluster_state}}

    def kql(self, command, database=None):
        self.events.append(("kql", command.split()[0]))

    def collection(self, path, **kwargs):
        return self.databases if path.endswith("/databases") else self.containers

    def blobs(self, container):
        return [{"etag": f'"{item["name"]}"', **item} for item in self.items[container]]

    def paths(self, container):
        return [{"name": name, "isDirectory": True, "etag": f'"{name}"'}
                for name in self.directories[container]]

    def delete_directory(self, container, name, etag=None):
        self.events.append(("DELETE_DIRECTORY", name))
        if etag != f'"{name}"':
            raise b.HttpFailure(412, "HTTP 412: condition not met")
        self.directories[container].remove(name)

    def control(self):
        return REAL_AZURE.control(self)

    def head_control(self):
        return dict(self.metadata), self.etag

    def set_control(self, metadata, etag):
        if etag != self.etag:
            raise b.ControlConflict("HTTP 412: metadata changed")
        self.metadata = dict(metadata)
        self.etag = str(uuid.uuid4())
        self.events.append(("control", metadata["resetstate"]))

    def cli(self, *args):
        self.events.append(("cli", args[2]))
        if args[:2] == ("rest", "--method") and args[2] == "delete":
            self.databases = []

    def delete_database(self):
        self.events.append(("arm-delete-db",))
        self.databases = []
        return 200, None

    def provider_operation(self, url):
        self.events.append(("arm-operation",))
        return "Succeeded"

    def blob(self, method, container, name, **kwargs):
        self.events.append((method, name))
        if method == "DELETE":
            current = [item for item in self.blobs(container) if item["name"] == name]
            expected = (kwargs.get("headers") or {}).get("If-Match")
            if current and expected is not None and current[0]["etag"] != expected:
                raise b.HttpFailure(412, "HTTP 412: condition not met")
            self.items[container] = [item for item in self.items[container] if item["name"] != name]


class ConfigTests(unittest.TestCase):
    def test_default_local_template_and_private_outputs(self):
        c = b.Config({"AZURE_SUBSCRIPTION_ID": SUB})
        self.assertEqual(c.group, "rust-playground")
        self.assertEqual((c.minimum, c.maximum), (2, 4))
        self.assertTrue(c.generated()["ARROWCOL_AZURE_TABLE_ROOT"].startswith("abfss://"))
        self.assertNotIn("AZURE_SUBSCRIPTION_ID", c.generated())
        self.assertEqual(c.account, b.Config({"AZURE_SUBSCRIPTION_ID": SUB}).account)

    def test_rejects_non_sandbox_group(self):
        with self.assertRaises(b.Failure):
            b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "AZURE_RESOURCE_GROUP": "production"})

    def test_rejects_invalid_subscription(self):
        with self.assertRaises(b.Failure):
            b.Config({"AZURE_SUBSCRIPTION_ID": "default"})

    def test_bounded_capacity(self):
        for values in ({"AZURE_KUSTO_MAX_INSTANCES": "9"},
                       {"AZURE_KUSTO_MIN_INSTANCES": "5", "AZURE_KUSTO_MAX_INSTANCES": "4"},
                       {"AZURE_KUSTO_QAP_HOT_DAYS": "0"}):
            with self.assertRaises(b.Failure):
                b.Config({"AZURE_SUBSCRIPTION_ID": SUB, **values})

    def test_optional_cluster_policy_tags_never_override_ownership(self):
        self.assertEqual(b.Config({"AZURE_SUBSCRIPTION_ID": SUB}).cluster_tags, {})
        tag = {"NRMS.KustoPlatform.Classification.1P": "Corp"}
        c = b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "AZURE_KUSTO_CLUSTER_TAGS": json.dumps(tag)})
        self.assertEqual(c.cluster_tags, tag)
        for raw in ("[]", "not-json", '{"sandboxId": "x"}', '{"Project": "x"}', '{"k": 1}'):
            with self.assertRaises(b.Failure):
                b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "AZURE_KUSTO_CLUSTER_TAGS": raw})

    def test_ownership_tolerates_policy_tags_but_requires_exact_owner(self):
        c = b.Config({"AZURE_SUBSCRIPTION_ID": SUB})
        mine = {**b.TAGS, "sandboxId": c.sandbox}
        b.owned({"tags": {**mine, "NRMS.KustoPlatform.Classification.1P": "Corp"}}, c)
        for tags in ({**mine, "sandboxId": "other"}, {"project": "rust-playground"}, {}):
            with self.assertRaises(b.Failure):
                b.owned({"tags": tags}, c)
        with self.assertRaises(b.Failure):
            b.owned({}, c)

    def test_rejects_same_container(self):
        with self.assertRaises(b.Failure):
            b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "AZURE_STORAGE_CHECKPOINT_CONTAINER": "arrowcol"})

    def test_application_operator_requires_distinct_explicit_application_id(self):
        with self.assertRaises(b.Failure):
            b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "AZURE_OPERATOR_TYPE": "App"})
        c = b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "AZURE_OPERATOR_TYPE": "App",
                      "AZURE_OPERATOR_APPLICATION_ID": SUB})
        self.assertEqual(c.operator_application, SUB)

    def test_isolated_root_preserves_only_owned_account_container(self):
        c = b.Config({"AZURE_SUBSCRIPTION_ID": SUB})
        root = f"abfss://{c.data}@{c.account}.dfs.core.windows.net/loadtest/run-example/"
        isolated = b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "ARROWCOL_AZURE_TABLE_ROOT": root})
        self.assertEqual(isolated.table_prefix, "loadtest/run-example")
        self.assertEqual(isolated.generated()["ARROWCOL_AZURE_TABLE_ROOT"], root)
        for unsafe in (root.replace(c.account, "foreign"), root + "../escape",
                       root + "?token=secret", root.replace(c.data + "@", "foreign@")):
            with self.assertRaises(b.Failure):
                b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "ARROWCOL_AZURE_TABLE_ROOT": unsafe})

    def test_config_parser_does_not_execute_expansion(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("# comment\nexport AZURE_LOCATION='eastus2'\nVALUE=$(echo secret)\n")
            values = b.load_env(path)
            self.assertEqual(values["VALUE"], "$(echo secret)")
            self.assertEqual(values["AZURE_LOCATION"], "eastus2")

    def test_parser_rejects_invalid_assignment(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            for content in ("NOT AN ASSIGNMENT", "BAD-KEY=1", 'KEY="unclosed'):
                path.write_text(content)
                with self.assertRaises(b.Failure):
                    b.load_env(path)

    def test_private_generated_file_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env.azure"
            b.write_private(path, {"EXAMPLE": "value"})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(path.read_text(), "EXAMPLE=value\n")
            self.assertEqual(list(path.parent.iterdir()), [path])

    def test_redacts_identifiers_endpoints_and_principals(self):
        c = b.Config({"AZURE_SUBSCRIPTION_ID": SUB})
        text = c.redact(f"{SUB} {c.account} {c.cluster} https://secret.example/path user@example.com Bearer secret")
        for private in (SUB, c.account, c.cluster, "secret.example", "user@example.com", "Bearer secret"):
            self.assertNotIn(private, text)


class ResetTests(unittest.TestCase):
    def setUp(self):
        self.azure = FakeAzure()
        self.quiet = contextlib.redirect_stdout(io.StringIO())
        self.quiet.__enter__()
        self.directory = tempfile.TemporaryDirectory()
        self.record = Path(self.directory.name) / ".env.azure.reset.json"
        self.record_patch = patch.object(b, "RESET_RECORD", self.record)
        self.record_patch.__enter__()

    def tearDown(self):
        self.record_patch.__exit__(None, None, None)
        self.directory.cleanup()
        self.quiet.__exit__(None, None, None)

    def receipt(self, generation=None):
        return json.dumps({"stopped": True, "scope": "workspace-owned-loadtest", "dataPreserved": True,
                           "resetGeneration": generation or self.azure.metadata["resetgeneration"]}) + "\n"

    def test_preview_has_no_mutations(self):
        b.reset(self.azure, False, None)
        self.assertEqual(self.azure.events, [])

    def test_execute_requires_actual_stop_command(self):
        with self.assertRaises(b.Failure):
            b.reset(self.azure, True, None)
        self.assertEqual(self.azure.events, [])

    def test_public_execute_always_fails_closed_even_with_stop_receipt(self):
        with patch.object(b, "run_tree") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = self.receipt()
            with self.assertRaisesRegex(b.Failure, "Destructive reset is disabled"):
                b.reset(self.azure, True, ["stop"])
            run.assert_not_called()
        self.assertEqual(self.azure.events, [])

    def test_ownership_mismatch_fails_before_deletion(self):
        self.azure.tags["sandboxId"] = "another"
        with self.assertRaises(b.Failure):
            b.reset(self.azure, True, ["false"])
        self.assertEqual(self.azure.events, [])

    def test_refuses_unexpected_database(self):
        self.azure.databases.append({"name": "not-owned"})
        with self.assertRaises(b.Failure):
            b.reset_inventory(self.azure)

    def test_refuses_unexpected_container(self):
        self.azure.containers.append({"name": "not-owned", "properties": {}})
        with self.assertRaises(b.Failure):
            b.reset_inventory(self.azure)

    def test_refuses_foreign_container_metadata(self):
        self.azure.containers[0]["properties"]["metadata"]["sandboxid"] = "wrong"
        with self.assertRaises(b.Failure):
            b.reset_inventory(self.azure)

    def test_refuses_retained_copies(self):
        for setting in ({"isVersioningEnabled": True},
                        {"deleteRetentionPolicy": {"enabled": True}},
                        {"containerDeleteRetentionPolicy": {"enabled": True}},
                        {"restorePolicy": {"enabled": True}}):
            self.azure.settings = setting
            with self.assertRaises(b.Failure):
                b.reset_inventory(self.azure)

    def test_refuses_preexisting_soft_deleted_objects(self):
        self.azure.items[self.azure.c.data][0]["deleted"] = True
        with self.assertRaises(b.Failure):
            b.reset_inventory(self.azure)

    def test_refuses_malformed_inventory_scope(self):
        for name in ("/foreign/blob", "../foreign", "table/../../foreign", "table\\foreign", ""):
            self.azure.items[self.azure.c.data][0]["name"] = name
            with self.assertRaises(b.Failure):
                b.reset_inventory(self.azure)

    def test_refuses_active_cloud_test_occupancy(self):
        self.azure.metadata["occupancy"] = "testing"
        with self.assertRaises(b.Failure):
            b.reset(self.azure, True, ["stop"])
        self.assertEqual(self.azure.events, [])

    @patch.object(b, "run_tree")
    def test_stop_failure_retains_fence_without_deleting_data(self, run):
        run.return_value.returncode = 1
        with self.assertRaises(b.Failure):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.events, [("control", "fenced-v1")])
        self.assertEqual(self.azure.metadata["resetstate"], "fenced-v1")

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_reset_fences_quiesces_then_deletes_every_blob_and_database(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = self.receipt()
        before = self.azure.metadata["resetgeneration"]
        b._unqualified_reset_sequence(self.azure, ["stop"])
        sleep.assert_called_once_with(60)
        self.assertEqual(self.azure.events[0], ("control", "fenced-v1"))
        self.assertIn(("DELETE", "table/data.parquet"), self.azure.events)
        self.assertNotEqual(self.azure.metadata["resetgeneration"], before)
        self.assertEqual(sum(len(items) for items in self.azure.items.values()), 0)
        self.assertEqual(self.azure.databases, [])
        self.assertEqual(self.azure.metadata["resetstate"], "fenced-v1")
        self.assertEqual(self.azure.metadata["publishers"], "{}")
        self.assertFalse(any(len(event) > 1 and "fence.json" in event[1] for event in self.azure.events))
        b.reset(self.azure, False, None)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_physical_emptiness_failure_is_visible(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = self.receipt()
        with patch.object(self.azure, "blob"):
            with self.assertRaisesRegex(b.Failure, "left workload"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.metadata["resetstate"], "cleaning-v1")

    @patch.object(b, "run_tree")
    def test_success_exit_without_verified_stop_receipt_is_not_enough(self, run):
        run.return_value.returncode = 0
        run.return_value.stdout = "not stopped"
        with self.assertRaisesRegex(b.Failure, "did not verify"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.events, [("control", "fenced-v1")])

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_removes_empty_hns_directories_deepest_first(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = self.receipt()
        self.azure.directories[self.azure.c.data] = ["table", "table/empty", "table/empty/deeper"]
        b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.directories[self.azure.c.data], [])
        directories = [event[1] for event in self.azure.events if event[0] == "DELETE_DIRECTORY"]
        self.assertEqual(directories, ["table/empty/deeper", "table/empty", "table"])

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_late_inflight_publication_is_inventoried_after_grace(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = self.receipt()
        sleep.side_effect = lambda _: self.azure.items[self.azure.c.data].append({
            "name": "table/late.parquet", "version": None, "snapshot": None, "deleted": False,
        })
        b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertIn(("DELETE", "table/late.parquet"), self.azure.events)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_fence_change_during_quiescence_refuses_all_deletion(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = self.receipt()
        sleep.side_effect = lambda _: self.azure.metadata.update(resetgeneration=str(uuid.uuid4()))
        with self.assertRaisesRegex(b.Failure, "during quiescence"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertFalse(any(event[0] in ("DELETE", "DELETE_DIRECTORY", "cli") for event in self.azure.events))

    # --- gap 5: generation-bound runner stop receipt ---------------------------------

    @patch.object(b, "run_tree")
    def test_stop_receipt_must_bind_captured_generation(self, run):
        run.return_value.returncode = 0
        generation = self.azure.metadata["resetgeneration"]
        legacy = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        for stdout in (self.receipt(str(uuid.uuid4())), legacy, self.receipt(generation.upper())):
            run.return_value.stdout = stdout
            with self.assertRaisesRegex(b.Failure, "this reset generation"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
            self.assertEqual(run.call_args.kwargs["env"]["ARROWCOL_RESET_GENERATION"], generation)
        self.assertEqual(self.azure.events, [("control", "fenced-v1")])
        self.assertFalse(self.record.exists())

    # --- gap 2: reset never starts/stops; stop-vs-reset in both orders ---------------

    def run_stop(self):
        def cli(*args):
            self.azure.events.append(("arm-stop",))
            self.azure.cluster_state = "Stopped"
        with patch.object(self.azure, "cli", side_effect=cli), \
                patch.object(b, "retry_ready", lambda action, label, timeout=600: action()):
            b.stop_cluster(self.azure)

    def assert_no_deletion(self):
        self.assertFalse(any(event[0] in ("DELETE", "DELETE_DIRECTORY", "cli", "arm-delete-db")
                             for event in self.azure.events), self.azure.events)

    @patch.object(b, "run_tree")
    def test_non_running_cluster_refused_before_fence(self, run):
        for state in ("Stopped", "Stopping", "Starting"):
            self.azure.cluster_state = state
            with self.assertRaisesRegex(b.Failure, "Running"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        run.assert_not_called()
        self.assertEqual(self.azure.events, [])

    @patch.object(b, "run_tree")
    def test_stop_first_held_or_completed_reset_has_no_effect(self, run):
        self.azure.metadata.update(occupancy="stopping", occupancyid=str(uuid.uuid4()))
        with self.assertRaisesRegex(b.Failure, "stop hold"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.azure.metadata.update(occupancy="released")
        self.azure.metadata.pop("occupancyid")
        self.run_stop()
        before = list(self.azure.events)
        with self.assertRaisesRegex(b.Failure, "Running"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        run.assert_not_called()
        self.assertEqual(self.azure.events, before)
        self.assertEqual(self.azure.metadata["resetstate"], "ready-v1")

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_stop_completing_during_quiescence_aborts_claim_before_deletion(self, run, sleep):
        run.return_value.returncode, run.return_value.stdout = 0, self.receipt()
        generation = self.azure.metadata["resetgeneration"]
        sleep.side_effect = lambda _: self.run_stop()  # fenced-v1+empty: stop may claim
        with self.assertRaisesRegex(b.Failure, "a stop won"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assert_no_deletion()
        self.assertEqual(self.azure.events.count(("arm-stop",)), 1)
        self.assertEqual([e for e in self.azure.events if e[0] == "control"][-2:],
                         [("control", "cleaning-v1"), ("control", "fenced-v1")])
        self.assertEqual((self.azure.metadata["resetstate"], self.azure.metadata["resetgeneration"],
                          self.azure.metadata["publishers"], self.azure.metadata["occupancy"]),
                         ("fenced-v1", generation, "{}", "released"))
        self.assertFalse(self.record.exists())

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_stop_hold_in_flight_at_claim_refuses_definitely(self, run, sleep):
        run.return_value.returncode, run.return_value.stdout = 0, self.receipt()
        sleep.side_effect = lambda _: self.azure.metadata.update(
            occupancy="stopping", occupancyid=str(uuid.uuid4()))
        with self.assertRaisesRegex(b.Failure, "during quiescence"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assert_no_deletion()
        self.assertEqual(self.azure.metadata["resetstate"], "fenced-v1")
        self.assertFalse(self.record.exists())

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_cleanup_claimed_first_stop_is_refused_without_arm(self, run, sleep):
        run.return_value.returncode, run.return_value.stdout = 0, self.receipt()
        original, refused = self.azure.blob, []
        def blob(method, *args, **kwargs):
            if method == "DELETE" and not refused:
                with self.assertRaisesRegex(b.Failure, "Cleanup owns"):
                    self.run_stop()
                refused.append(True)
            return original(method, *args, **kwargs)
        with patch.object(self.azure, "blob", side_effect=blob):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(refused, [True])
        self.assertNotIn(("arm-stop",), self.azure.events)
        self.assertEqual(self.azure.cluster_state, "Running")

    # --- gap 1: persisted custody record and read-only-first resume -------------------

    def start_reset(self, run):
        run.return_value.returncode, run.return_value.stdout = 0, self.receipt()
        return self.azure.metadata["resetgeneration"]

    def assert_clean_finish(self, record):
        self.assertFalse(self.record.exists())
        self.assertEqual((self.azure.metadata["resetstate"], self.azure.metadata["resetgeneration"],
                          self.azure.metadata["publishers"]), ("fenced-v1", record["next"], "{}"))
        self.assertEqual(sum(len(items) for items in self.azure.items.values()), 0)
        self.assertEqual(self.azure.databases, [])

    def test_fresh_sequence_refuses_when_custody_record_exists(self):
        b.write_reset_record(self.record, {"sandbox": self.azure.c.sandbox, "operator": str(uuid.uuid4()),
                                           "generation": str(uuid.uuid4()), "next": str(uuid.uuid4())})
        with self.assertRaisesRegex(b.Failure, "resume"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.events, [])

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_record_persisted_before_claim_and_interruption_after_claim_resumes(self, run, sleep):
        generation = self.start_reset(run)
        original = self.azure.set_control
        def set_control(metadata, etag):
            if metadata["resetstate"] == "cleaning-v1":
                record = b.read_reset_record(self.record)  # durable BEFORE claim CAS
                self.assertEqual(record["generation"], generation)
                self.assertEqual(json.loads(metadata["publishers"]),
                                 {record["operator"]: {"generation": generation, "state": "active"}})
            return original(metadata, etag)
        def crash(*args, **kwargs):
            raise KeyboardInterrupt
        with patch.object(self.azure, "set_control", side_effect=set_control), \
                patch.object(self.azure, "blob", side_effect=crash):
            with self.assertRaises(KeyboardInterrupt):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        record = b.read_reset_record(self.record)
        self.assertEqual(self.azure.metadata["resetstate"], "cleaning-v1")
        self.assertEqual(oct(self.record.stat().st_mode & 0o777), "0o600")
        b._unqualified_reset_resume(self.azure)
        self.assert_clean_finish(record)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_ambiguous_claim_not_applied_reclaims_same_operator_on_resume(self, run, sleep):
        self.start_reset(run)
        original = self.azure.set_control
        def lost(metadata, etag):
            if metadata["resetstate"] == "cleaning-v1":
                raise b.Failure("timeout before apply")
            return original(metadata, etag)
        with patch.object(self.azure, "set_control", side_effect=lost):
            with self.assertRaisesRegex(b.Failure, "outcome unknown"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assert_no_deletion()
        self.assertEqual(self.azure.metadata["resetstate"], "fenced-v1")
        record = b.read_reset_record(self.record)
        b._unqualified_reset_resume(self.azure)
        self.assert_clean_finish(record)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_ambiguous_claim_applied_continues_without_new_identity(self, run, sleep):
        self.start_reset(run)
        original = self.azure.set_control
        def applied_then_lost(metadata, etag):
            original(metadata, etag)
            if metadata["resetstate"] == "cleaning-v1":
                raise b.Failure("response lost after apply")
        with patch.object(self.azure, "set_control", side_effect=applied_then_lost):
            with self.assertRaisesRegex(b.Failure, "outcome unknown"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assert_no_deletion()
        record = b.read_reset_record(self.record)
        self.assertEqual(list(json.loads(self.azure.metadata["publishers"])), [record["operator"]])
        b._unqualified_reset_resume(self.azure)
        self.assert_clean_finish(record)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_applied_delete_with_lost_response_is_observed_not_replayed(self, run, sleep):
        self.start_reset(run)
        original = self.azure.blob
        def lost(method, container, name, **kwargs):
            original(method, container, name, **kwargs)
            raise b.Failure("DELETE response lost")
        with patch.object(self.azure, "blob", side_effect=lost):
            with self.assertRaisesRegex(b.Failure, "response lost"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        record = b.read_reset_record(self.record)
        self.assertEqual(self.azure.metadata["resetstate"], "cleaning-v1")
        b._unqualified_reset_resume(self.azure)
        self.assertEqual(self.azure.events.count(("DELETE", "table/data.parquet")), 1)
        self.assert_clean_finish(record)

    # --- database DELETE: durable intent + provider operation receipt -----------------

    OPERATION = "https://management.azure.com/providers/op/1"

    def intent(self):
        path = b.database_intent_path(self.record)
        return json.loads(path.read_bytes()) if path.exists() else None

    def assert_unresumable(self, generation):
        self.assertEqual((self.azure.metadata["resetstate"], self.azure.metadata["resetgeneration"]),
                         ("cleaning-v1", generation))
        self.assertTrue(self.record.exists())
        self.assertEqual(self.intent()["state"], "submitting")

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_accepted_database_delete_finishes_only_on_provider_success(self, run, sleep):
        self.start_reset(run)
        statuses = ["InProgress", "InProgress", "Succeeded"]
        def submit():
            self.azure.events.append(("arm-delete-db",))
            self.assertEqual(self.intent()["state"], "submitting")  # durable BEFORE submission
            self.azure.databases = []  # listing absence alone must not settle it
            return 202, self.OPERATION
        with patch.object(self.azure, "delete_database", side_effect=submit), \
                patch.object(self.azure, "provider_operation", side_effect=lambda url: statuses.pop(0)), \
                patch.object(b, "retry_ready", lambda action, label, timeout=600: action()):
            with self.assertRaisesRegex(b.Failure, "still in progress"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
            record = b.read_reset_record(self.record)
            self.assertEqual((self.intent()["state"], self.intent()["operation"]), ("accepted", self.OPERATION))
            with self.assertRaisesRegex(b.Failure, "still in progress"):
                b._unqualified_reset_resume(self.azure)
            self.assertEqual(self.azure.metadata["resetstate"], "cleaning-v1")
            b._unqualified_reset_resume(self.azure)
        self.assertEqual(self.azure.events.count(("arm-delete-db",)), 1)
        self.assertIsNone(self.intent())
        self.assert_clean_finish(record)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_delayed_unknown_database_delete_then_absent_listing_refuses_finish(self, run, sleep):
        """An unknown DELETE may land after rotation+reprovision; absence never reconciles it."""
        generation = self.start_reset(run)
        def lost():
            self.azure.events.append(("arm-delete-db",))
            raise b.Failure("timed out; outcome unknown")
        with patch.object(self.azure, "delete_database", side_effect=lost):
            with self.assertRaisesRegex(b.Failure, "UNRESUMABLE"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assert_unresumable(generation)
        self.azure.databases = []  # the delayed request applied, or not: listing cannot tell
        for _ in range(2):
            with self.assertRaisesRegex(b.Failure, "UNRESUMABLE"):
                b._unqualified_reset_resume(self.azure)
            self.assert_unresumable(generation)
        self.assertEqual(self.azure.events.count(("arm-delete-db",)), 1)  # never replayed
        from publisher_control import PublisherControl
        with self.assertRaisesRegex(b.Failure, "cleanup-owned"):  # no rebootstrap admission
            PublisherControl(self.azure).admit(str(uuid.uuid4()), generation, bootstrap=True)
        with self.assertRaisesRegex(b.Failure, "resume"):
            self.record.rename(self.record.with_suffix(".moved"))
            b._unqualified_reset_sequence(self.azure, ["stop"])

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_crash_during_database_submission_is_unresumable(self, run, sleep):
        generation = self.start_reset(run)
        def crash():
            self.azure.databases = []
            raise KeyboardInterrupt
        with patch.object(self.azure, "delete_database", side_effect=crash):
            with self.assertRaises(KeyboardInterrupt):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        with self.assertRaisesRegex(b.Failure, "UNRESUMABLE"):
            b._unqualified_reset_resume(self.azure)
        self.assert_unresumable(generation)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_accepted_without_provider_receipt_is_unresumable(self, run, sleep):
        generation = self.start_reset(run)
        for response in ((202, None), (202, "https://elsewhere.example/op"), (201, None)):
            with self.subTest(response=response):
                with patch.object(self.azure, "delete_database", return_value=response):
                    with self.assertRaisesRegex(b.Failure, "UNRESUMABLE"):
                        if self.record.exists():
                            b._unqualified_reset_resume(self.azure)
                        else:
                            b._unqualified_reset_sequence(self.azure, ["stop"])
                self.assert_unresumable(generation)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_provider_terminal_failure_permits_single_resubmission(self, run, sleep):
        self.start_reset(run)
        submitted = []
        def submit():
            submitted.append(True)
            if len(submitted) == 2:
                self.azure.databases = []
                return 200, None
            return 202, self.OPERATION
        with patch.object(self.azure, "delete_database", side_effect=submit), \
                patch.object(self.azure, "provider_operation", return_value="Failed"):
            with self.assertRaisesRegex(b.Failure, "Failed; resume may resubmit"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
            self.assertIsNone(self.intent())
            record = b.read_reset_record(self.record)
            b._unqualified_reset_resume(self.azure)
        self.assertEqual(len(submitted), 2)
        self.assert_clean_finish(record)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_database_deleting_without_own_intent_or_foreign_intent_is_refused(self, run, sleep):
        self.start_reset(run)
        self.azure.databases = [{"name": self.azure.c.database,
                                 "properties": {"provisioningState": "Deleting"}}]
        with self.assertRaisesRegex(b.Failure, "without this custody's intent"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        record = b.read_reset_record(self.record)
        foreign = {"operator": str(uuid.uuid4()), "generation": record["generation"],
                   "state": "completed", "operation": None}
        b.publish_private(b.database_intent_path(self.record), foreign)
        self.azure.databases = []
        with self.assertRaisesRegex(b.Failure, "never adopting"):
            b._unqualified_reset_resume(self.azure)
        self.assertNotIn(("arm-delete-db",), self.azure.events)
        self.assertEqual(self.azure.metadata["resetstate"], "cleaning-v1")

    def test_real_transport_captures_status_and_provider_operation(self):
        azure = REAL_AZURE(b.Config({"AZURE_SUBSCRIPTION_ID": SUB}))
        with patch.object(azure, "request", return_value=(b"", {":status": "202",
                                                               "azure-asyncoperation": self.OPERATION})) as request:
            self.assertEqual(azure.delete_database(), (202, self.OPERATION))
        self.assertEqual(request.call_args.args[0], "DELETE")
        with patch.object(azure, "request", return_value=(None, {})):
            self.assertEqual(azure.delete_database(), (404, None))
        with patch.object(azure, "request", return_value=(b"", {":status": "202", "location": self.OPERATION})):
            self.assertEqual(azure.delete_database(), (202, None))  # Location-only is not a receipt
        for response, expected in (
                ((b'{"status":"Succeeded"}', {":status": "200"}), "Succeeded"),
                ((b'{"status":"succeeded"}', {":status": "200"}), "Succeeded"),
                ((b'{"status":"Failed"}', {":status": "200"}), "Failed"),
                ((b'{"status":"Canceled"}', {":status": "200"}), "Canceled"),
                ((b'{"status":"InProgress"}', {":status": "200"}), "InProgress"),
                ((b"", {":status": "200"}), "Unknown"),
                ((b"{}", {":status": "200"}), "Unknown"),
                ((b"not json", {":status": "200"}), "Unknown"),
                ((b"[]", {":status": "200"}), "Unknown"),
                ((b'{"status":{"value":"Succeeded"}}', {":status": "200"}), "Unknown"),
                ((b'{"status":null}', {":status": "200"}), "Unknown"),
                ((b'{"status":"Queued"}', {":status": "200"}), "Unknown"),
                ((b'{"status":"Running"}', {":status": "200"}), "Unknown"),
                ((b'{"status":"Succeeded"}', {":status": "202"}), "Unknown"),
                ((b"", {":status": "204"}), "Unknown")):
            with self.subTest(response=response), patch.object(azure, "request", return_value=response):
                self.assertEqual(azure.provider_operation(self.OPERATION), expected)

    def test_operation_url_is_strictly_validated_before_any_token(self):
        azure = REAL_AZURE(b.Config({"AZURE_SUBSCRIPTION_ID": SUB}))
        bad = ["http://management.azure.com/op", "https://management.azure.com.evil.example/op",
               "https://user@management.azure.com/op", "https://management.azure.com:8443/op",
               "https://evil.example/op", "https://management.azure.com/op#frag",
               "https://management.azure.com/a/../op", "https://management.azure.com/op x",
               "https://management.azure.com/op\nX", "https://management.azure.com\\@evil/op",
               "https://MANAGEMENT.azure.com/op", None, 7, "https://management.azure.com/" + "a" * 2100]
        with patch.object(azure, "token", side_effect=AssertionError("token fetched")), \
                patch.object(azure, "cli", side_effect=AssertionError("cli used")):
            for url in bad:
                with self.subTest(url=url), self.assertRaisesRegex(b.Failure, "operation URL"):
                    azure.provider_operation(url)
        self.assertEqual(b.operation_url(self.OPERATION + "?api-version=2024-04-13"),
                         self.OPERATION + "?api-version=2024-04-13")

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_unknown_provider_receipt_retains_accepted_without_removal_or_resubmit(self, run, sleep):
        generation = self.start_reset(run)
        for status in ("Unknown", "Queued", "Running", None, {"status": "Succeeded"}, ""):
            with self.subTest(status=status):
                with patch.object(self.azure, "delete_database", return_value=(202, self.OPERATION)) as submit, \
                        patch.object(self.azure, "provider_operation", return_value=status), \
                        patch.object(b, "retry_ready", lambda action, label, timeout=600: action()):
                    with self.assertRaisesRegex(b.Failure, "receipt is unknown"):
                        if self.record.exists():
                            b._unqualified_reset_resume(self.azure)
                        else:
                            b._unqualified_reset_sequence(self.azure, ["stop"])
                self.assertEqual(self.intent()["state"], "accepted")
                self.assertEqual((self.azure.metadata["resetstate"], self.azure.metadata["resetgeneration"]),
                                 ("cleaning-v1", generation))
                self.assertTrue(self.record.exists())
        self.assertEqual(self.azure.events.count(("arm-delete-db",)), 0)  # patched: submitted once only
        self.assertEqual(submit.call_count, 0)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_tampered_persisted_operation_url_is_refused_on_resume(self, run, sleep):
        generation = self.start_reset(run)
        with patch.object(self.azure, "delete_database", return_value=(202, self.OPERATION)), \
                patch.object(self.azure, "provider_operation", return_value="InProgress"), \
                patch.object(b, "retry_ready", lambda action, label, timeout=600: action()):
            with self.assertRaisesRegex(b.Failure, "in progress"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        accepted = self.intent()
        tampered = {**accepted, "operation": "https://evil.example/op"}
        b.publish_private(b.database_intent_path(self.record), tampered, previous=accepted)
        with patch.object(self.azure, "provider_operation", side_effect=AssertionError("polled")), \
                patch.object(self.azure, "delete_database", side_effect=AssertionError("resubmitted")):
            with self.assertRaisesRegex(b.Failure, "operation URL"):
                b._unqualified_reset_resume(self.azure)
        self.assertEqual(self.intent(), tampered)
        self.assertEqual(self.azure.metadata["resetgeneration"], generation)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_ambiguous_finish_is_recognized_by_persisted_successor(self, run, sleep):
        self.start_reset(run)
        original = self.azure.set_control
        def applied_then_lost(metadata, etag):
            original(metadata, etag)
            if metadata["resetstate"] == "fenced-v1" and json.loads(metadata["publishers"]) == {} \
                    and self.record.exists() and \
                    metadata["resetgeneration"] == b.read_reset_record(self.record)["next"]:
                raise b.Failure("finish response lost")
        with patch.object(self.azure, "set_control", side_effect=applied_then_lost):
            with self.assertRaisesRegex(b.Failure, "definitive success"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        record = b.read_reset_record(self.record)
        events = list(self.azure.events)
        b._unqualified_reset_resume(self.azure)
        self.assertEqual(self.azure.events, events)
        self.assert_clean_finish(record)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_emptiness_failure_keeps_custody_and_record(self, run, sleep):
        self.start_reset(run)
        with patch.object(self.azure, "blob"):
            with self.assertRaisesRegex(b.Failure, "left workload"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertTrue(self.record.exists())
        self.assertEqual(self.azure.metadata["resetstate"], "cleaning-v1")

    def test_resume_never_takes_over_foreign_uncertain_or_other_generation(self):
        generation = self.azure.metadata["resetgeneration"]
        operator = str(uuid.uuid4())
        record = {"sandbox": self.azure.c.sandbox, "operator": operator,
                  "generation": generation, "next": str(uuid.uuid4())}
        b.write_reset_record(self.record, record)
        cases = {
            "uncertain own": ("cleaning-v1", generation, {operator: "uncertain"}),
            "foreign": ("cleaning-v1", generation, {str(uuid.uuid4()): "active"}),
            "own plus foreign": ("cleaning-v1", generation, {operator: "active", str(uuid.uuid4()): "active"}),
            "fenced with member": ("fenced-v1", generation, {str(uuid.uuid4()): "active"}),
            "ready same generation": ("ready-v1", generation, {}),
        }
        for label, (state, gen, members) in cases.items():
            with self.subTest(label):
                publishers = {key: {"generation": gen, "state": value} for key, value in members.items()}
                self.azure.metadata.update(resetstate=state, resetgeneration=gen,
                                           publishers=json.dumps(publishers, separators=(",", ":"), sort_keys=True))
                before = dict(self.azure.metadata)
                with self.assertRaisesRegex(b.Failure, "never taking over"):
                    b._unqualified_reset_resume(self.azure)
                self.assertEqual(self.azure.metadata, before)
        other = str(uuid.uuid4())
        self.azure.metadata.update(resetstate="cleaning-v1", resetgeneration=other, publishers=json.dumps(
            {operator: {"generation": other, "state": "active"}}, separators=(",", ":")))
        with self.assertRaisesRegex(b.Failure, "neither"):
            b._unqualified_reset_resume(self.azure)
        self.record.unlink()
        b.write_reset_record(self.record, {**record, "sandbox": "another"})
        with self.assertRaisesRegex(b.Failure, "another sandbox"):
            b._unqualified_reset_resume(self.azure)
        self.assertEqual(self.azure.events, [])
        self.assertTrue(self.record.exists())

    # --- local custody record: exclusive, create-if-absent, durable --------------------

    def claim_hook(self, during_claim):
        original = self.azure.set_control
        def set_control(metadata, etag):
            if metadata["resetstate"] == "cleaning-v1" and not getattr(self, "hooked", False):
                self.hooked = True
                during_claim()
            return original(metadata, etag)
        return patch.object(self.azure, "set_control", side_effect=set_control)

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_second_reset_process_is_excluded_by_lock_between_publish_and_claim(self, run, sleep):
        self.start_reset(run)
        seen = {}
        def competitor():
            seen["bytes"] = self.record.read_bytes()
            with self.assertRaisesRegex(b.Failure, "custody lock"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
            with self.assertRaisesRegex(b.Failure, "custody lock"):
                b._unqualified_reset_resume(self.azure)
            self.assertEqual(self.record.read_bytes(), seen["bytes"])
        with self.claim_hook(competitor):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertIn("bytes", seen)
        self.assert_clean_finish(json.loads(seen["bytes"]))

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_toctou_loser_never_overwrites_or_unlinks_winner_record(self, run, sleep):
        """Even with the lock bypassed, link() create-if-absent keeps the winner's record."""
        self.start_reset(run)
        seen = {}
        def competitor():  # read-before-publish raced: it saw no record
            seen["bytes"] = self.record.read_bytes()
            with patch.object(b, "read_reset_record", return_value=None):
                with self.assertRaisesRegex(b.Failure, "never overwriting"):
                    b._unqualified_reset_sequence(self.azure, ["stop"])
            self.assertEqual(self.record.read_bytes(), seen["bytes"])
            self.assertEqual([p.name for p in self.record.parent.iterdir()
                              if p.name.startswith(".env.azure.reset.") and not p.name.endswith(".lock")
                              and p != self.record], [])
        with patch.object(b, "reset_lock", lambda path: contextlib.nullcontext()), \
                self.claim_hook(competitor):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assert_clean_finish(json.loads(seen["bytes"]))

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_definite_claim_loser_never_removes_record_it_does_not_own(self, run, sleep):
        self.start_reset(run)
        foreign = {"sandbox": self.azure.c.sandbox, "operator": str(uuid.uuid4()),
                   "generation": self.azure.metadata["resetgeneration"], "next": str(uuid.uuid4())}
        def replaced_then_refused(operator, generation):
            self.record.unlink()
            b.write_reset_record(self.record, foreign)
            raise b.Failure("definitely refused")
        with patch("publisher_control.PublisherControl.claim_cleanup", side_effect=replaced_then_refused):
            with self.assertRaisesRegex(b.Failure, "never removing"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.record.read_bytes(), b.record_bytes(foreign))
        with self.assertRaisesRegex(b.Failure, "never removing"):
            b.remove_reset_record(self.record, {**foreign, "operator": str(uuid.uuid4())})
        self.assertEqual(self.record.read_bytes(), b.record_bytes(foreign))

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_parent_fsync_failure_propagates_before_claim_and_resume_repairs(self, run, sleep):
        self.start_reset(run)
        real = b.fsync_directory
        failing = b.Failure("Cleanup custody record directory could not be made durable")
        with patch.object(b, "fsync_directory", side_effect=failing):
            with self.assertRaisesRegex(b.Failure, "made durable"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
            self.assertNotIn(("control", "cleaning-v1"), self.azure.events)
            self.assertEqual(self.azure.metadata["resetstate"], "fenced-v1")
            with self.assertRaisesRegex(b.Failure, "made durable"):
                b._unqualified_reset_resume(self.azure)
            self.assertNotIn(("control", "cleaning-v1"), self.azure.events)
        record = b.read_reset_record(self.record)
        with patch.object(b, "fsync_directory", wraps=real) as fsync:
            b._unqualified_reset_resume(self.azure)
        self.assertEqual(fsync.call_args_list[0].args, (self.record.parent,))  # repair first
        self.assertGreaterEqual(fsync.call_count, 2)  # and after the durable removal
        self.assert_clean_finish(record)

    def test_record_publish_fsyncs_file_then_parent_and_is_canonical(self):
        record = {"sandbox": self.azure.c.sandbox, "operator": str(uuid.uuid4()),
                  "generation": str(uuid.uuid4()), "next": str(uuid.uuid4())}
        order = []
        real_fsync = os.fsync
        with patch.object(b.os, "fsync", side_effect=lambda fd: (order.append(fd), real_fsync(fd))), \
                patch.object(b.os, "link", wraps=os.link) as link:
            b.write_reset_record(self.record, record)
        self.assertEqual(len(order), 2)  # temp file, then parent directory after link
        link.assert_called_once()
        self.assertEqual(self.record.read_bytes(), b.record_bytes(record))
        self.assertEqual(self.record.stat().st_mode & 0o777, 0o600)
        with self.assertRaisesRegex(b.Failure, "never overwriting"):
            b.write_reset_record(self.record, {**record, "operator": str(uuid.uuid4())})
        self.assertEqual(self.record.read_bytes(), b.record_bytes(record))
        with patch.object(b.os, "fsync", side_effect=OSError("EIO")):
            with self.assertRaisesRegex(b.Failure, "made durable"):
                b.remove_reset_record(self.record, record)

    # --- ambiguous DELETE: physical reconciliation, conditional on exact object -------

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_timed_out_unapplied_delete_is_reissued_only_for_same_object(self, run, sleep):
        self.start_reset(run)
        original, lost = self.azure.blob, []
        def timeout(method, container, name, **kwargs):
            if method == "DELETE" and not lost:
                lost.append((container, name, kwargs))
                self.azure.events.append((method, name))
                raise b.Failure("timed out; outcome unknown")
            return original(method, container, name, **kwargs)
        with patch.object(self.azure, "blob", side_effect=timeout):
            with self.assertRaisesRegex(b.Failure, "outcome unknown"):
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(lost[0][2]["headers"]["If-Match"], '"table/data.parquet"')
        record = b.read_reset_record(self.record)
        b._unqualified_reset_resume(self.azure)
        self.assertEqual(self.azure.events.count(("DELETE", "table/data.parquet")), 2)
        self.assert_clean_finish(record)
        # The stale in-flight request lands after rotation and a successor write at the path.
        container, name, kwargs = lost[0]
        self.azure.items[container].append({"name": name, "etag": '"successor"', "snapshot": None,
                                            "version": None, "deleted": False})
        with self.assertRaises(b.HttpFailure) as stale:
            self.azure.blob("DELETE", container, name, **kwargs)
        self.assertEqual(stale.exception.code, 412)
        self.assertEqual([item["etag"] for item in self.azure.items[container]], ['"successor"'])

    @patch.object(b.time, "sleep")
    @patch.object(b, "run_tree")
    def test_object_without_etag_is_never_deleted_unconditionally(self, run, sleep):
        self.start_reset(run)
        self.azure.items[self.azure.c.data][0]["etag"] = None
        with self.assertRaisesRegex(b.Failure, "no ETag"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assert_no_deletion()
        self.assertEqual(self.azure.metadata["resetstate"], "cleaning-v1")
        self.assertTrue(self.record.exists())

    def test_real_listing_captures_blob_etag(self):
        azure = REAL_AZURE(b.Config({"AZURE_SUBSCRIPTION_ID": SUB}))
        listing = (b"<EnumerationResults><Blobs><Blob><Name>t/x</Name>"
                   b"<Properties><Etag>0x8D1</Etag></Properties></Blob></Blobs></EnumerationResults>")
        with patch.object(azure, "blob", return_value=(listing, {})):
            self.assertEqual(azure.blobs("c")[0]["etag"], "0x8D1")
        with patch.object(azure, "request") as request:
            azure.delete_directory("c", "t", '"e"')
        self.assertEqual(request.call_args.kwargs["headers"]["If-Match"], '"e"')


class OccupancyTests(unittest.TestCase):
    def test_occupy_exports_exact_authority_generation_privately_not_run_uuid(self):
        azure = FakeAzure()
        owner = str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / ".env.azure"
            with contextlib.redirect_stdout(io.StringIO()):
                b.occupancy(azure, "occupy", owner, output)
            values = b.load_env(output)
            self.assertEqual(values["ARROWCOL_RESET_GENERATION"], azure.metadata["resetgeneration"])
            self.assertNotEqual(values["ARROWCOL_RESET_GENERATION"], owner)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)

    def test_generation_change_after_occupy_cas_does_not_export_admission(self):
        azure = FakeAzure()
        original = azure.set_control
        def change(metadata, etag):
            original(metadata, etag)
            azure.metadata["resetgeneration"] = str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / ".env.azure"
            with patch.object(azure, "set_control", side_effect=change):
                with self.assertRaisesRegex(b.Failure, "Authority changed"):
                    b.occupancy(azure, "occupy", str(uuid.uuid4()), output)
            self.assertFalse(output.exists())

    def test_only_exact_test_owner_can_release(self):
        azure = FakeAzure()
        owner = str(uuid.uuid4())
        with contextlib.redirect_stdout(io.StringIO()):
            b.occupancy(azure, "occupy", owner)
            self.assertEqual(azure.metadata["occupancy"], "testing")
            with self.assertRaises(b.Failure):
                b.occupancy(azure, "release", str(uuid.uuid4()))
            with self.assertRaises(b.Failure):
                b.occupancy(azure, "occupy", str(uuid.uuid4()))
            b.occupancy(azure, "release", owner, generation=azure.metadata["resetgeneration"])
        self.assertEqual(azure.metadata["occupancy"], "released")

    def stop(self, azure, during_stop=None):
        def cli(*args):
            azure.events.append(("cli", args[2]))
            if during_stop:
                during_stop()
        with patch.object(azure, "cli", side_effect=cli), \
                patch.object(b, "retry_ready", lambda action, label, timeout: None):
            b.stop_cluster(azure)

    def test_occupy_before_stop_blocks_stop_before_any_arm_call(self):
        azure = FakeAzure()
        with contextlib.redirect_stdout(io.StringIO()):
            b.occupancy(azure, "occupy", str(uuid.uuid4()))
        with self.assertRaises(b.Failure):
            self.stop(azure)
        self.assertNotIn(("cli", "post"), azure.events)
        self.assertEqual(azure.metadata["occupancy"], "testing")

    def test_stop_hold_rejects_occupy_during_stop_then_releases(self):
        azure = FakeAzure()
        rejected = []
        def compete():
            try:
                b.occupancy(azure, "occupy", str(uuid.uuid4()))
            except b.Failure:
                rejected.append(azure.metadata["occupancy"])
        with contextlib.redirect_stdout(io.StringIO()):
            self.stop(azure, compete)
        self.assertEqual(rejected, ["stopping"])
        self.assertEqual(azure.metadata["occupancy"], "released")

    def test_occupy_racing_stop_hold_cas_prevents_arm_stop(self):
        azure = FakeAzure()
        original = azure.set_control
        def occupy_first(metadata, etag):
            with patch.object(azure, "set_control", side_effect=original), \
                    contextlib.redirect_stdout(io.StringIO()):
                b.occupancy(azure, "occupy", str(uuid.uuid4()))
            original(metadata, etag)
        with patch.object(azure, "set_control", side_effect=occupy_first):
            with self.assertRaises(b.Failure):
                self.stop(azure)
        self.assertNotIn(("cli", "post"), azure.events)
        self.assertEqual(azure.metadata["occupancy"], "testing")

    def test_unknown_arm_stop_outcome_keeps_blocking_hold_without_replay(self):
        azure = FakeAzure()
        with patch.object(azure, "cli", side_effect=b.Failure("ARM timeout")):
            with self.assertRaises(b.Failure):
                b.stop_cluster(azure)
        self.assertEqual(azure.metadata["occupancy"], "stopping")
        holder = azure.metadata["occupancyid"]
        for candidate in (str(uuid.uuid4()), holder):
            with self.assertRaises(b.Failure):  # neither tests nor release by stop-id
                b.occupancy(azure, "occupy" if candidate != holder else "release", candidate)
        with self.assertRaises(b.Failure):
            b.require_released(azure.metadata)
        azure.events.clear()
        with self.assertRaisesRegex(b.Failure, "ambiguous"):
            self.stop(azure)
        self.assertNotIn(("cli", "post"), azure.events)
        self.assertEqual(azure.metadata["occupancy"], "stopping")
        self.assertEqual(azure.metadata["occupancyid"], holder)

    def test_unknown_stop_wait_keeps_hold(self):
        azure = FakeAzure()
        with patch.object(azure, "cli", return_value=None), \
                patch.object(b, "retry_ready", side_effect=b.Failure("timeout")):
            with self.assertRaises(b.Failure):
                b.stop_cluster(azure)
        self.assertEqual(azure.metadata["occupancy"], "stopping")

    def test_definite_hold_cas_conflict_writes_nothing_and_stays_retryable(self):
        azure = FakeAzure()
        with patch.object(azure, "set_control", side_effect=b.ControlConflict("busy")):
            with self.assertRaises(b.Failure):
                self.stop(azure)
        self.assertNotIn(("cli", "post"), azure.events)
        self.assertEqual(azure.metadata.get("occupancy", "released"), "released")
        self.stop(azure)
        self.assertEqual(azure.metadata["occupancy"], "released")

    def test_cli_timeout_kills_process_group_and_reports_unknown(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "child-survived"
            script = f"(sleep 2; touch {marker}) & sleep 30"
            started = time.monotonic()
            with self.assertRaisesRegex(b.Failure, "outcome unknown"):
                b.run_tree(["bash", "-c", script], 0.5)
            self.assertLess(time.monotonic() - started, 10)
            time.sleep(2.5)
            self.assertFalse(marker.exists())
    def test_fenced_sandbox_cannot_be_occupied(self):
        azure = FakeAzure()
        azure.metadata["resetstate"] = "fenced-v1"
        with self.assertRaises(b.Failure):
            b.occupancy(azure, "occupy", str(uuid.uuid4()))

    def test_compare_exchange_rejects_stale_etag(self):
        azure = FakeAzure()
        metadata, etag = azure.control()
        azure.etag = '"concurrent-update"'
        with self.assertRaises(b.Failure):
            azure.set_control(metadata, etag)


class ReconcileTests(unittest.TestCase):
    def test_missing_delta_root_prevents_all_kql(self):
        azure = FakeAzure()
        azure.blob = lambda *args, **kwargs: (None, {})
        azure.kql = lambda *args, **kwargs: self.fail("KQL ran before valid Delta root")
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            manifest.write_text('["Telemetry"]')
            with self.assertRaises(b.NotReady):
                b.reconcile(azure, manifest)

    def test_waits_only_for_absent_logs_not_authentication_failure(self):
        azure = FakeAzure()
        azure.blob = unittest.mock.Mock(side_effect=b.Failure("HTTP 403: denied"))
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            manifest.write_text('["Telemetry"]')
            with patch.object(b.time, "sleep") as sleep:
                with self.assertRaisesRegex(b.Failure, "HTTP 403"):
                    b.reconcile(azure, manifest, 30)
                sleep.assert_not_called()
        self.assertEqual(azure.blob.call_count, 1)

    def test_missing_log_becomes_ready_within_bounded_wait(self):
        azure = FakeAzure()
        azure.blob = unittest.mock.Mock(side_effect=[
            (None, {}), (b'{"protocol":{}}\n{"metaData":{}}', {}),
        ])
        azure.arm = lambda *args, **kwargs: {"properties": {"principalId": SUB}}
        azure.kql = unittest.mock.Mock()
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            manifest.write_text('["Telemetry"]')
            with patch.object(b.time, "sleep") as sleep, contextlib.redirect_stdout(io.StringIO()):
                b.reconcile(azure, manifest, 10)
            sleep.assert_called_once()
        self.assertEqual(azure.kql.call_count, 3)

    def test_unsafe_table_name_is_rejected(self):
        azure = FakeAzure()
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            manifest.write_text(json.dumps(["bad'); .drop database x"]))
            with self.assertRaises(b.Failure):
                b.reconcile(azure, manifest)

    def test_unsafe_relative_root_is_rejected(self):
        azure = FakeAzure()
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            for path in ("../foreign", "/absolute", "https://foreign", "table;token=secret"):
                manifest.write_text(json.dumps([{"name": "Telemetry", "relative_path": path}]))
                with self.assertRaises(b.Failure):
                    b.reconcile(azure, manifest)

    def test_empty_root_policy_order_and_managed_identity_usage(self):
        azure = FakeAzure()
        azure.blob = lambda *args, **kwargs: (
            b'{"protocol":{"minReaderVersion":1}}\n{"metaData":{"schemaString":"{}"}}', {})
        azure.arm = lambda *args, **kwargs: {"properties": {"principalId": SUB}}
        commands = []
        azure.kql = lambda command: commands.append(command)
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            manifest.write_text('["Telemetry"]')
            with contextlib.redirect_stdout(io.StringIO()):
                b.reconcile(azure, manifest)
        self.assertIn("ExternalTable,AutomatedFlows", commands[0])
        self.assertIn(".create-or-alter external table", commands[1])
        self.assertIn("policy query_acceleration", commands[2])
        self.assertIn('"ManagedIdentity"', commands[2])

    def test_both_pools_use_inferred_normalized_schema_at_catalog_paths(self):
        azure = FakeAzure()
        roots, commands = [], []
        def blob(method, container, name, **kwargs):
            roots.append(name)
            return b'{"protocol":{}}\n{"metaData":{}}', {}
        azure.blob = blob
        azure.arm = lambda *args, **kwargs: {"properties": {"principalId": SUB}}
        azure.kql = lambda command: commands.append(command)
        bindings = [{"name": pool + "_table", "relative_path": pool + "/telemetry/sqlserver/table"}
                    for pool in ("direct", "dataflow")]
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            manifest.write_text(json.dumps(bindings))
            with contextlib.redirect_stdout(io.StringIO()):
                b.reconcile(azure, manifest)
        self.assertEqual(len(commands), 5)
        self.assertTrue(all("/telemetry/sqlserver/table/_delta_log/" in root for root in roots))
        self.assertIn("['direct_table'] kind=delta", commands[1])
        self.assertIn("['dataflow_table'] kind=delta", commands[3])

    def test_isolated_root_prefix_is_used_for_log_validation_and_kql(self):
        azure = FakeAzure()
        azure.c.table_prefix = "loadtest/run-example"
        roots, commands = [], []
        def blob(method, container, name, **kwargs):
            roots.append(name)
            return b'{"protocol":{}}\n{"metaData":{}}', {}
        azure.blob = blob
        azure.arm = lambda *args, **kwargs: {"properties": {"principalId": SUB}}
        azure.kql = lambda command: commands.append(command)
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "tables.json"
            manifest.write_text('[{"name":"direct_table","relative_path":"direct/catalog/schema/table"}]')
            with contextlib.redirect_stdout(io.StringIO()):
                b.reconcile(azure, manifest)
        self.assertTrue(roots[0].startswith("loadtest/run-example/direct/"))
        self.assertIn("/loadtest/run-example/direct/catalog/schema/table;", commands[1])


class StorageApiTests(unittest.TestCase):
    def setUp(self):
        self.azure = b.Azure(b.Config({"AZURE_SUBSCRIPTION_ID": SUB}))

    def leased_storage(self, **metadata):
        c = self.azure.c
        state = {"metadata": {"sandboxid": c.sandbox, "resetstate": "ready-v1",
                              "resetgeneration": str(uuid.uuid4()), "project": "rust-playground",
                              "occupancy": "released", "publisherregistryversion": "1",
                              "publishers": "{}",
                              **metadata},
                 "lease": None, "calls": [], "etag": 1}
        def blob(method, container, name="", query=None, body=None, headers=None, missing_ok=False):
            query, headers = query or {}, headers or {}
            self.assertNotIn("If-Match", headers)
            action = headers.get("x-ms-lease-action")
            state["calls"].append(action or (method, query.get("comp")))
            if method == "HEAD":
                return b"", {**{"x-ms-meta-" + k: v for k, v in state["metadata"].items()},
                             "etag": f'"{state["etag"]}"'}
            if action == "acquire":
                if state["lease"]:
                    raise b.HttpFailure(409, "HTTP 409: LeaseAlreadyPresent")
                self.assertEqual(headers["x-ms-lease-duration"], str(b.LEASE_SECONDS))
                state["lease"] = headers["x-ms-proposed-lease-id"]
                return b"", {}
            if action == "release":
                if headers["x-ms-lease-id"] == state["lease"]:
                    state["lease"] = None
                return b"", {}
            if query.get("comp") == "metadata":
                if headers.get("x-ms-lease-id") != state["lease"] or not state["lease"]:
                    raise b.HttpFailure(412, "HTTP 412: LeaseIdMismatch")
                state["metadata"] = {k[10:]: v for k, v in headers.items() if k.startswith("x-ms-meta-")}
                state["etag"] += 1
                return b"", {}
            raise AssertionError((method, query, headers))
        return state, blob

    def test_metadata_write_is_lease_guarded_read_compare_write_release(self):
        state, blob = self.leased_storage()
        with patch.object(self.azure, "blob", side_effect=blob):
            metadata, etag = self.azure.control()
            metadata["resetstate"] = "fenced"
            self.azure.set_control(metadata, etag)
        self.assertEqual(state["calls"], [("HEAD", None), "acquire", ("HEAD", None),
                                          ("PUT", "metadata"), "release"])
        self.assertEqual(state["metadata"]["resetstate"], "fenced")
        self.assertEqual(state["metadata"]["project"], "rust-playground")
        self.assertIsNone(state["lease"])

    def test_busy_lease_writes_nothing(self):
        state, blob = self.leased_storage()
        with patch.object(self.azure, "blob", side_effect=blob):
            metadata, etag = self.azure.control()
            state["lease"] = str(uuid.uuid4())
            with self.assertRaises(b.ControlConflict):
                self.azure.set_control({**metadata, "resetstate": "fenced"}, etag)
        self.assertNotIn(("PUT", "metadata"), state["calls"])
        self.assertEqual(state["metadata"]["resetstate"], "ready-v1")

    def test_concurrent_change_between_read_and_lease_writes_nothing_and_releases(self):
        state, blob = self.leased_storage()
        with patch.object(self.azure, "blob", side_effect=blob):
            metadata, etag = self.azure.control()
            state["metadata"]["occupancy"] = "testing"  # competing leased writer won
            with self.assertRaises(b.ControlConflict):
                self.azure.set_control({**metadata, "resetstate": "fenced"}, etag)
        self.assertNotIn(("PUT", "metadata"), state["calls"])
        self.assertEqual(state["calls"][-1], "release")
        self.assertIsNone(state["lease"])

    def test_lost_lease_412_is_definitely_not_applied(self):
        state, blob = self.leased_storage()
        def expire_then(method, container, **kwargs):
            if (kwargs.get("query") or {}).get("comp") == "metadata":
                state["lease"] = str(uuid.uuid4())  # expired and re-acquired by another writer
            return blob(method, container, **kwargs)
        with patch.object(self.azure, "blob", side_effect=expire_then):
            metadata, etag = self.azure.control()
            with self.assertRaises(b.ControlConflict):
                self.azure.set_control({**metadata, "resetstate": "fenced"}, etag)
        self.assertEqual(state["metadata"]["resetstate"], "ready-v1")

    def test_unknown_write_failure_is_not_reported_as_conflict(self):
        state, blob = self.leased_storage()
        def timeout(method, container, **kwargs):
            if (kwargs.get("query") or {}).get("comp") == "metadata":
                blob(method, container, **kwargs)
                raise b.Failure("timed out")
            return blob(method, container, **kwargs)
        with patch.object(self.azure, "blob", side_effect=timeout):
            metadata, etag = self.azure.control()
            with self.assertRaises(b.Failure) as raised:
                self.azure.set_control({**metadata, "resetstate": "fenced"}, etag)
        self.assertNotIsInstance(raised.exception, b.ControlConflict)
        self.assertEqual(state["calls"][-1], "release")

    def test_write_without_prior_read_is_refused(self):
        state, blob = self.leased_storage()
        with patch.object(self.azure, "blob", side_effect=blob):
            with self.assertRaises(b.ControlConflict):
                self.azure.set_control({"resetstate": "fenced"}, '"unread"')
        self.assertEqual(state["calls"], [])
    def test_missing_ownership_state_generation_or_etag_fails_closed(self):
        c = self.azure.c
        complete = {"x-ms-meta-sandboxid": c.sandbox, "x-ms-meta-resetstate": "ready-v1",
                    "x-ms-meta-resetgeneration": str(uuid.uuid4()),
                    "x-ms-meta-occupancy": "released", "x-ms-meta-publisherregistryversion": "1",
                    "x-ms-meta-publishers": "{}", "etag": '"one"'}
        with patch.object(self.azure, "blob", return_value=(b"", complete)):
            self.azure.control()
        for missing in complete:
            headers = {key: value for key, value in complete.items() if key != missing}
            with patch.object(self.azure, "blob", return_value=(b"", headers)):
                with self.assertRaises(b.Failure):
                    self.azure.control()

    def test_blob_inventory_paginates_and_includes_retained_copies(self):
        first = b"<EnumerationResults><Blobs><Blob><Name>one</Name><Snapshot>s</Snapshot></Blob></Blobs><NextMarker>next</NextMarker></EnumerationResults>"
        second = b"<EnumerationResults><Blobs><Blob><Name>two</Name><VersionId>v</VersionId><Deleted>true</Deleted></Blob></Blobs><NextMarker/></EnumerationResults>"
        with patch.object(self.azure, "blob", side_effect=[(first, {}), (second, {})]) as blob:
            entries = self.azure.blobs(self.azure.c.data)
            self.assertEqual(len(entries), 2)
            self.assertEqual(entries[0]["snapshot"], "s")
            self.assertEqual(entries[1]["version"], "v")
            self.assertTrue(entries[1]["deleted"])
            self.assertEqual(blob.call_args.kwargs["query"]["marker"], "next")
            self.assertIn("versions", blob.call_args.kwargs["query"]["include"])

    def test_hns_path_inventory_paginates_empty_directories(self):
        pages = [(b'{"paths":[{"name":"empty","isDirectory":true}]}',
                  {"x-ms-continuation": "more"}),
                 (b'{"paths":[{"name":"another","isDirectory":true}]}', {})]
        with patch.object(self.azure, "request", side_effect=pages) as request:
            paths = self.azure.paths(self.azure.c.data)
            self.assertEqual(len(paths), 2)
            self.assertIn("continuation=more", request.call_args.args[1])

    def test_arm_pagination_refuses_foreign_scope(self):
        with patch.object(self.azure, "arm", return_value={
            "value": [], "nextLink": "https://foreign.invalid/?token=secret",
        }):
            with self.assertRaises(b.Failure):
                self.azure.collection(self.azure.c.cluster_id + "/databases")

    def test_oauth_storage_request_includes_azure_date_and_version(self):
        response = unittest.mock.MagicMock()
        response.__enter__.return_value.read.return_value = b""
        response.__enter__.return_value.headers = {}
        with patch.object(self.azure, "token", return_value="test-token"), \
                patch.object(b.urllib.request, "urlopen", return_value=response) as urlopen:
            self.azure.blob("HEAD", self.azure.c.checkpoints, query={"restype": "container"})
        headers = dict(urlopen.call_args.args[0].header_items())
        self.assertIn("X-ms-date", headers)
        self.assertEqual(headers["X-ms-version"], "2023-11-03")
        self.assertEqual(headers["Authorization"], "Bearer test-token")


class InitializerHookTests(unittest.TestCase):
    def test_hook_invokes_only_owned_initializer_with_private_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            cargo = path / "cargo"
            cargo.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\"\n")
            cargo.chmod(0o700)
            env = {
                **os.environ, "PATH": directory + ":" + os.environ["PATH"],
                "ARROWCOL_AZURE_TABLE_MANIFEST": str(path / ".env.azure.tables.json"),
                "ARROWCOL_AZURE_TABLE_ROOT": "abfss://data@example.dfs.core.windows.net/",
                "AZURE_STORAGE_ACCOUNT": "example", "AZURE_STORAGE_CONTAINER": "data",
                "AZURE_STORAGE_CHECKPOINT_CONTAINER": "checkpoints",
                "ARROWCOL_EXPECTED_RESET_GENERATION": str(uuid.uuid4()),
            }
            result = b.subprocess.run(
                ["bash", str(b.ROOT / "infra/azure/initialize-delta.sh")], env=env,
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.splitlines(), [
                "run", "--locked", "--quiet", "--package", "arrowcol-loadgen",
                "--", "initialize-azure", "--manifest", env["ARROWCOL_AZURE_TABLE_MANIFEST"],
            ])

    def test_hook_missing_private_configuration_fails_before_cargo(self):
        env = {key: value for key, value in os.environ.items()
               if key not in ("ARROWCOL_AZURE_TABLE_MANIFEST", "ARROWCOL_AZURE_TABLE_ROOT")}
        result = b.subprocess.run(
            ["bash", str(b.ROOT / "infra/azure/initialize-delta.sh")], env=env,
            capture_output=True, text=True, check=False,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("bootstrap must provide", result.stderr)


    def test_hook_requires_captured_expected_generation_before_cargo(self):
        with tempfile.TemporaryDirectory() as directory:
            cargo = Path(directory) / "cargo"
            cargo.write_text("#!/bin/sh\necho CARGO_RAN\n")
            cargo.chmod(0o700)
            env = {key: value for key, value in os.environ.items()
                   if key != "ARROWCOL_EXPECTED_RESET_GENERATION"}
            env.update({
                "PATH": directory + ":" + os.environ["PATH"],
                "ARROWCOL_AZURE_TABLE_MANIFEST": str(Path(directory) / "m.json"),
                "ARROWCOL_AZURE_TABLE_ROOT": "abfss://data@example.dfs.core.windows.net/",
                "AZURE_STORAGE_ACCOUNT": "example", "AZURE_STORAGE_CONTAINER": "data",
                "AZURE_STORAGE_CHECKPOINT_CONTAINER": "checkpoints",
            })
            result = b.subprocess.run(
                ["bash", str(b.ROOT / "infra/azure/initialize-delta.sh")], env=env,
                capture_output=True, text=True, check=False,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("CARGO_RAN", result.stdout)
        self.assertIn("captured reset generation", result.stderr)


class RunTreeTests(unittest.TestCase):
    def script(self, directory):
        marker = Path(directory) / "grandchild-survived"
        return marker, ["bash", "-c", f"(sleep 2; touch {marker}) & sleep 30"]

    def test_outer_interruption_kills_process_group_and_reraises(self):
        with tempfile.TemporaryDirectory() as directory:
            marker, command = self.script(directory)
            original = b.subprocess.Popen.communicate
            def interrupted(process, timeout=None):
                if timeout is not None:
                    raise KeyboardInterrupt
                return original(process)
            with patch.object(b.subprocess.Popen, "communicate", interrupted):
                with self.assertRaises(KeyboardInterrupt):
                    b.run_tree(command, 60)
            time.sleep(2.5)
            self.assertFalse(marker.exists())

    def test_runner_stop_timeout_kills_tree_and_keeps_fence_blocking(self):
        azure = FakeAzure()
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(b, "reset_inventory", return_value=([], {})):
            marker, command = self.script(directory)
            real = b.run_tree
            with patch.object(b, "run_tree", lambda cmd, timeout, **kw: real(cmd, 0.5, **kw)), \
                    contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(b.Failure, "outcome unknown"):
                    b._unqualified_reset_sequence(azure, command, Path(directory) / "reset.json")
            time.sleep(2.5)
            self.assertFalse(marker.exists())
        self.assertEqual(azure.metadata["resetstate"], "fenced-v1")

class BicepContractTests(unittest.TestCase):
    def test_kusto_application_principal_is_distinct_from_storage_object_principal(self):
        source = (b.ROOT / "infra/azure/sandbox.bicep").read_text()
        reader = source.split("resource reader ", 1)[1]
        storage = source.split("resource readData ", 1)[1].split("resource writeData ", 1)[0]
        self.assertIn("principalId: identity.properties.clientId", reader)
        self.assertIn("principalId: identity.properties.principalId", storage)

    def test_redeploy_does_not_replace_live_container_control_metadata(self):
        source = (b.ROOT / "infra/azure/sandbox.bicep").read_text()
        container = source.split("resource checkpoints ", 1)[1].split("resource readData ", 1)[0]
        self.assertIn("if (!checkpointExists)", container)


class ProvisionCoordinationTests(unittest.TestCase):
    """Dispatched entrypoints (provision/reconcile/stop/occupy/release) ownership matrix."""

    def setUp(self):
        from publisher_control import PublisherControl
        self.azure = FakeAzure()
        self.control = PublisherControl(self.azure)
        self.directory = tempfile.TemporaryDirectory()
        self.output = Path(self.directory.name) / ".env.azure"
        self.tables = Path(self.directory.name) / "tables.json"
        self.tables.write_text('["Logs"]')
        self.deployed = []
        self.observed = []
        self.child = {}
        self.stop_unknown = False
        self.patches = [
            patch.object(b, "guard_existing"),
            patch.object(b, "retry_ready", lambda action, label, timeout=600: action()),
            patch.object(b, "reconcile", side_effect=self.fake_reconcile),
            patch.object(b, "run_tree", side_effect=self.fake_child),
            patch.object(self.azure, "cli", side_effect=self.fake_cli),
            contextlib.redirect_stdout(io.StringIO()),
        ]
        for item in self.patches:
            item.__enter__()

    def tearDown(self):
        for item in reversed(self.patches):
            item.__exit__(None, None, None)
        self.directory.cleanup()

    def registry(self):
        return json.loads(self.azure.metadata["publishers"])

    def fake_cli(self, *args):
        if args[:3] == ("deployment", "sub", "create"):
            params = json.loads(Path(args[-1][1:]).read_text())["parameters"]
            self.deployed.append((params["checkpointExists"]["value"], params["controlMetadata"]["value"],
                                  dict(self.registry()) if self.azure.checkpoint_exists else None))
            if not params["checkpointExists"]["value"]:
                self.azure.metadata = {**params["controlMetadata"]["value"],
                                       "sandboxid": self.azure.c.sandbox, "project": "rust-playground"}
                self.azure.checkpoint_exists = True
            self.azure.events.append(("arm", "deploy"))
            return None
        operation = args[-1].split("?")[0].rsplit("/", 1)[-1]
        self.azure.events.append(("arm", operation))
        self.observed.append((operation, self.azure.metadata["resetstate"], dict(self.registry())))
        if operation == "start":
            self.azure.cluster_state = "Running"
        if operation == "stop":
            if self.stop_unknown:
                raise b.Failure("ARM stop outcome unknown")
            self.azure.cluster_state = "Stopped"

    def fake_reconcile(self, azure, tables, wait_seconds=0):
        self.observed.append(("kql", azure.metadata["resetstate"], dict(self.registry())))
        azure.events.append(("kql", "reconcile"))

    def fake_child(self, command, timeout, cwd=None, env=None):
        generation = env["ARROWCOL_EXPECTED_RESET_GENERATION"]
        child = str(uuid.uuid4())
        self.child["generation"], self.child["id"] = generation, child
        # Rust child: admits own UUID at the expected generation, drains, unregisters.
        self.control.admit(child, generation, bootstrap=True)
        self.control.clean_release(child, generation, definitively_drained=True)
        if self.child.get("then"):
            self.child["then"]()
        result = type("Result", (), {})()
        result.returncode, result.stdout, result.stderr = 0, "", ""
        return result

    def provision(self, infra_only=False):
        b.provision(self.azure, self.tables, 0, self.output, infra_only, {})

    def test_fresh_container_initializes_v1_then_parent_member_owns_start_and_kql(self):
        self.azure.checkpoint_exists, self.azure.cluster_state = False, "Stopped"
        self.provision()
        exists, fresh, _ = self.deployed[0]
        self.assertFalse(exists)
        self.assertEqual(fresh["resetstate"], "fenced-v1")
        self.assertEqual((fresh["occupancy"], fresh["publishers"], fresh["publisherregistryversion"]),
                         ("released", "{}", "1"))
        self.assertEqual(str(uuid.UUID(fresh["resetgeneration"])), fresh["resetgeneration"])
        self.assertEqual(self.child["generation"], fresh["resetgeneration"])
        for operation, state, registry in self.observed:  # start and KQL
            self.assertEqual(state, "fenced-v1")
            self.assertEqual(len(registry), 1, operation)
            self.assertNotIn(self.child["id"], registry)
        self.assertEqual([item[0] for item in self.observed], ["start", "kql"])
        self.assertEqual(self.azure.metadata["resetstate"], "ready-v1")
        self.assertEqual(self.azure.metadata["resetgeneration"], fresh["resetgeneration"])
        self.assertEqual(self.registry(), {})
        self.assertEqual(b.load_env(self.output)["ARROWCOL_RESET_GENERATION"], fresh["resetgeneration"])

    def test_redeploy_preserves_all_container_state_under_registered_member(self):
        self.azure.metadata["custom"] = "preserved"
        before = dict(self.azure.metadata)
        self.provision()
        exists, metadata, registry = self.deployed[0]
        self.assertTrue(exists)
        self.assertEqual(metadata, {})
        self.assertEqual(len(registry), 1)  # deploy ran under exclusive operator custody
        self.assertEqual(self.azure.metadata, before)
        self.assertEqual([event[1] for event in self.azure.events if event[0] == "control"],
                         ["fenced-v1", "ready-v1"])
        self.assertTrue(all(state == "fenced-v1" and len(members) == 1
                            for _, state, members in self.observed))
        self.assertNotIn("generation", self.child)  # no rotation or re-initialization
        self.assertEqual(b.load_env(self.output)["ARROWCOL_RESET_GENERATION"], before["resetgeneration"])

    def test_legacy_or_unversioned_authority_is_refused_without_migration(self):
        for legacy in ({"resetstate": "ready"}, {"resetstate": "fenced"}):
            azure = FakeAzure()
            azure.metadata = {"sandboxid": azure.c.sandbox, "resetgeneration": str(uuid.uuid4()), **legacy}
            before = dict(azure.metadata)
            with patch.object(azure, "cli") as cli:
                for entry in (lambda: b.provision(azure, self.tables, 0, self.output, False, {}),
                              lambda: b.reconcile_command(azure, self.tables, ""),
                              lambda: b.stop_cluster(azure),
                              lambda: b.occupancy(azure, "occupy", str(uuid.uuid4()))):
                    with self.assertRaisesRegex(b.Failure, "Legacy or unversioned"):
                        entry()
                cli.assert_not_called()
            self.assertEqual(azure.metadata, before)
            self.assertEqual(azure.events, [])

    def test_cleanup_owned_or_nonempty_fenced_authority_refuses_provision(self):
        for state, publishers in (("cleaning-v1", {str(uuid.uuid4()): "active"}),
                                  ("fenced-v1", {str(uuid.uuid4()): "uncertain"})):
            generation = self.azure.metadata["resetgeneration"]
            self.azure.metadata.update(resetstate=state, publishers=json.dumps(
                {k: {"generation": generation, "state": v} for k, v in publishers.items()}))
            with self.assertRaises(b.Failure):
                self.provision()
            self.assertNotIn(("arm", "deploy"), self.azure.events)

    def test_stop_hold_first_blocks_bootstrap_before_any_io(self):
        self.azure.metadata["resetstate"] = "fenced-v1"
        self.stop_unknown = True
        with self.assertRaises(b.Failure):
            b.stop_cluster(self.azure)
        self.assertEqual(self.azure.metadata["occupancy"], "stopping")
        self.azure.events.clear()
        with self.assertRaises(b.Failure):
            self.provision()
        self.assertEqual([e for e in self.azure.events if e[0] in ("arm", "kql")], [])
        with self.assertRaises(b.Failure):
            self.control.admit(str(uuid.uuid4()), self.azure.metadata["resetgeneration"], bootstrap=True)

    def test_bootstrap_member_first_blocks_stop_before_arm(self):
        self.azure.metadata["resetstate"] = "fenced-v1"
        results = []
        def stop_during(azure, tables, wait_seconds=0):
            self.fake_reconcile(azure, tables, wait_seconds)
            try:
                b.stop_cluster(azure)
            except b.Failure as exc:
                results.append(str(exc))
        with patch.object(b, "reconcile", side_effect=stop_during):
            self.provision()
        self.assertEqual(len(results), 1)
        self.assertIn("protect the cluster", results[0])
        self.assertNotIn(("arm", "stop"), self.azure.events)
        self.assertEqual(self.azure.metadata["resetstate"], "ready-v1")

    def test_stop_between_child_drain_and_parent_admission_fails_parent_before_kql(self):
        self.azure.metadata["resetstate"] = "fenced-v1"
        self.stop_unknown = True
        def stop_now():
            with self.assertRaises(b.Failure):
                b.stop_cluster(self.azure)
        self.child["then"] = stop_now
        with self.assertRaises(b.Failure):
            self.provision()
        self.assertNotIn("start", [item[0] for item in self.observed])
        self.assertNotIn("kql", [item[0] for item in self.observed])
        self.assertEqual(self.registry(), {})
        self.assertEqual(self.azure.metadata["occupancy"], "stopping")

    def test_normal_member_blocks_stop_and_stop_hold_blocks_normal_admission(self):
        generation = self.azure.metadata["resetgeneration"]
        member = str(uuid.uuid4())
        self.control.admit(member, generation)
        with self.assertRaisesRegex(b.Failure, "protect the cluster"):
            b.stop_cluster(self.azure)
        self.assertNotIn(("arm", "stop"), self.azure.events)
        self.control.clean_release(member, generation, definitively_drained=True)
        self.stop_unknown = True
        with self.assertRaises(b.Failure):
            b.stop_cluster(self.azure)
        with self.assertRaisesRegex(b.Failure, "stop holds"):
            self.control.admit(str(uuid.uuid4()), generation)
        with self.assertRaises(b.Failure):
            b.reconcile_command(self.azure, self.tables, "")
        self.assertEqual(self.registry(), {})

    def hook_before(self, method, action):
        """Run a competing transition once immediately before the named CAS method."""
        from publisher_control import PublisherControl
        original = getattr(PublisherControl, method)
        fired = []
        def wrapper(control, *args, **kwargs):
            if not fired:
                fired.append(True)
                action()
            return original(control, *args, **kwargs)
        return patch.object(PublisherControl, method, wrapper)

    def hosted_io(self):
        return [e for e in self.azure.events if e[0] == "kql" or e in (("arm", "deploy"), ("arm", "start"))]

    def test_occupy_before_ready_provision_custody_fails_provision_before_io(self):
        other = str(uuid.uuid4())
        with self.hook_before("claim_exclusive", lambda: b.occupancy(self.azure, "occupy", other)):
            with self.assertRaises(b.Failure):
                self.provision()
        self.assertEqual(self.hosted_io(), [])
        self.assertEqual((self.azure.metadata["occupancy"], self.azure.metadata["occupancyid"]),
                         ("testing", other))
        self.assertEqual((self.azure.metadata["resetstate"], self.registry()), ("ready-v1", {}))

    def test_ready_provision_custody_first_rejects_occupy_until_ready(self):
        refused = []
        def deploy_then_occupy(*args):
            self.fake_cli(*args)
            if args[:3] == ("deployment", "sub", "create"):
                with self.assertRaisesRegex(b.Failure, "ready-v1"):
                    b.occupancy(self.azure, "occupy", str(uuid.uuid4()))
                refused.append(True)
        with patch.object(self.azure, "cli", side_effect=deploy_then_occupy):
            self.provision()
        self.assertEqual(refused, [True])
        self.assertEqual((self.azure.metadata["occupancy"], self.azure.metadata["resetstate"]),
                         ("released", "ready-v1"))

    def test_released_reconcile_races_occupy_in_both_orders(self):
        other = str(uuid.uuid4())
        with self.hook_before("claim_exclusive", lambda: b.occupancy(self.azure, "occupy", other)):
            with self.assertRaises(b.Failure):
                b.reconcile_command(self.azure, self.tables, "", 0, None)
        self.assertEqual(self.hosted_io(), [])
        b.occupancy(self.azure, "release", other, generation=self.azure.metadata["resetgeneration"])
        def occupy_during(azure, tables, wait_seconds=0):
            self.fake_reconcile(azure, tables, wait_seconds)
            with self.assertRaisesRegex(b.Failure, "ready-v1"):
                b.occupancy(azure, "occupy", str(uuid.uuid4()))
        with patch.object(b, "reconcile", side_effect=occupy_during):
            b.reconcile_command(self.azure, self.tables, "", 0, None)
        self.assertEqual((self.azure.metadata["occupancy"], self.azure.metadata["resetstate"],
                          self.registry()), ("released", "ready-v1", {}))

    def test_occupied_reconcile_pins_owner_inside_admission_both_orders(self):
        owner_a, owner_b = str(uuid.uuid4()), str(uuid.uuid4())
        generation = self.azure.metadata["resetgeneration"]
        b.occupancy(self.azure, "occupy", owner_a)
        def swap():
            b.occupancy(self.azure, "release", owner_a, generation=generation)
            b.occupancy(self.azure, "occupy", owner_b)
        with self.hook_before("admit", swap):
            with self.assertRaisesRegex(b.Failure, "owner changed"):
                b.reconcile_command(self.azure, self.tables, owner_a, 0, generation)
        self.assertEqual(self.hosted_io(), [])
        self.assertEqual((self.azure.metadata["occupancyid"], self.registry()), (owner_b, {}))
        b.occupancy(self.azure, "release", owner_b, generation=generation)
        b.occupancy(self.azure, "occupy", owner_a)
        def swap_during(azure, tables, wait_seconds=0):
            self.fake_reconcile(azure, tables, wait_seconds)
            with self.assertRaisesRegex(b.Failure, "retained"):
                b.occupancy(azure, "release", owner_a, generation=generation)
            with self.assertRaises(b.Failure):
                b.occupancy(azure, "occupy", owner_b)
        with patch.object(b, "reconcile", side_effect=swap_during):
            b.reconcile_command(self.azure, self.tables, owner_a, 0, generation)
        self.assertEqual((self.azure.metadata["occupancyid"], self.registry()), (owner_a, {}))

    def test_normal_collectors_and_same_owner_reoccupy_still_admitted(self):
        owner = str(uuid.uuid4())
        generation = self.azure.metadata["resetgeneration"]
        b.occupancy(self.azure, "occupy", owner)
        collector = str(uuid.uuid4())
        self.control.admit(collector, generation)
        b.occupancy(self.azure, "occupy", owner)
        b.reconcile_command(self.azure, self.tables, owner, 0, generation)
        self.assertEqual(set(self.registry()), {collector})
        self.control.clean_release(collector, generation, definitively_drained=True)
        b.occupancy(self.azure, "release", owner, generation=generation)

    def test_public_reconcile_holds_member_for_start_and_kql_then_releases(self):
        self.azure.cluster_state = "Stopped"
        b.reconcile_command(self.azure, self.tables, "")
        self.assertEqual([item[0] for item in self.observed], ["start", "kql"])
        self.assertTrue(all(len(item[2]) == 1 for item in self.observed))
        self.assertEqual(self.registry(), {})

    def test_occupied_reconcile_requires_captured_generation_and_forwards_wait(self):
        owner, generation = str(uuid.uuid4()), self.azure.metadata["resetgeneration"]
        b.occupancy(self.azure, "occupy", owner)
        for expected in (None, str(uuid.uuid4())):
            with self.assertRaisesRegex(b.Failure, "generation"):
                b.reconcile_command(self.azure, self.tables, owner, 120, expected)
        self.assertEqual(self.observed, [])
        self.assertEqual(self.registry(), {})
        waits = []
        with patch.object(b, "reconcile", side_effect=lambda a, t, w=0: waits.append(w)):
            b.reconcile_command(self.azure, self.tables, owner, 120, generation)
        self.assertEqual(waits, [120])
        self.assertEqual(self.registry(), {})
        self.assertEqual(self.azure.metadata["occupancy"], "testing")

    def test_unknown_arm_start_keeps_member_blocking_and_stop_refused(self):
        self.azure.cluster_state = "Stopped"
        def unknown(*args):
            self.fake_cli(*args)
            raise b.Failure("ARM start outcome unknown")
        with patch.object(self.azure, "cli", side_effect=unknown):
            with self.assertRaises(b.Failure):
                b.reconcile_command(self.azure, self.tables, "")
        self.azure.cluster_state = "Running"  # a Running snapshot is not drain proof
        self.assertEqual([entry["state"] for entry in self.registry().values()], ["uncertain"])
        with self.assertRaisesRegex(b.Failure, "protect the cluster"):
            b.stop_cluster(self.azure)
        self.assertNotIn(("arm", "stop"), self.azure.events)

    def test_not_ready_reconcile_clean_releases_member(self):
        with patch.object(b, "reconcile", side_effect=b.NotReady("absent")):
            with self.assertRaises(b.NotReady):
                b.reconcile_command(self.azure, self.tables, "")
        self.assertEqual(self.registry(), {})

    def test_release_requires_generation_owner_and_empty_registry(self):
        owner = str(uuid.uuid4())
        generation = self.azure.metadata["resetgeneration"]
        b.occupancy(self.azure, "occupy", owner)
        member = str(uuid.uuid4())
        self.control.admit(member, generation)
        for args in ((owner, generation), (owner, str(uuid.uuid4())), (owner, None),
                     (str(uuid.uuid4()), generation)):
            with self.assertRaises(b.Failure):
                b.occupancy(self.azure, "release", args[0], generation=args[1])
            self.assertEqual(self.azure.metadata["occupancy"], "testing")
        self.control.uncertain(member, generation)
        with self.assertRaisesRegex(b.Failure, "occupancy retained"):
            b.occupancy(self.azure, "release", owner, generation=generation)
        self.assertEqual(self.azure.metadata["occupancy"], "testing")
        azure = FakeAzure()
        from publisher_control import PublisherControl
        control, generation = PublisherControl(azure), azure.metadata["resetgeneration"]
        b.occupancy(azure, "occupy", owner)
        control.admit(member, generation)
        control.clean_release(member, generation, definitively_drained=True)
        b.occupancy(azure, "release", owner, generation=generation)
        self.assertEqual(azure.metadata["occupancy"], "released")

    @patch.dict(os.environ, {}, clear=True)
    def test_main_dispatches_v1_paths_and_refuses_legacy(self):
        env = Path(self.directory.name) / ".env"
        env.write_text(f"AZURE_SUBSCRIPTION_ID={SUB}\n")
        legacy = FakeAzure()
        legacy.metadata = {"sandboxid": legacy.c.sandbox, "resetstate": "ready",
                           "resetgeneration": str(uuid.uuid4())}
        stderr = io.StringIO()
        with patch.object(b, "Azure", return_value=legacy), patch.object(b, "preflight"), \
                patch.object(legacy, "cli") as cli, contextlib.redirect_stderr(stderr):
            for command in (["provision"], ["reconcile"], ["stop", "--execute"],
                            ["occupy", "--occupancy-id", str(uuid.uuid4())]):
                self.assertEqual(b.main([*command, "--env-file", str(env), "--output", str(self.output),
                                         "--tables", str(self.tables)]), 1)
            cli.assert_not_called()
        self.assertEqual(stderr.getvalue().count("Legacy or unversioned"), 4)
        owner = str(uuid.uuid4())
        with patch.object(b, "Azure", return_value=self.azure), patch.object(b, "preflight"):
            self.assertEqual(b.main(["occupy", "--occupancy-id", owner, "--env-file", str(env),
                                     "--output", str(self.output)]), 0)
            generation = b.load_env(self.output)["ARROWCOL_RESET_GENERATION"]
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(b.main(["release", "--occupancy-id", owner, "--env-file", str(env)]), 1)
            self.assertEqual(b.main(["release", "--occupancy-id", owner, "--reset-generation",
                                     generation, "--env-file", str(env)]), 0)
        self.assertEqual(self.azure.metadata["occupancy"], "released")

    @patch.dict(os.environ, {}, clear=True)
    def test_dispatched_entrypoints_have_no_effect_while_cleanup_owns_authority(self):
        """Gap 4: cleaning-v1 excludes every public mutator before ARM/KQL/blob I/O."""
        env = Path(self.directory.name) / ".env"
        env.write_text(f"AZURE_SUBSCRIPTION_ID={SUB}\n")
        generation, operator = self.azure.metadata["resetgeneration"], str(uuid.uuid4())
        self.azure.metadata.update(resetstate="cleaning-v1", publishers=json.dumps(
            {operator: {"generation": generation, "state": "active"}}, separators=(",", ":")))
        before = dict(self.azure.metadata)
        owner = str(uuid.uuid4())
        commands = (["provision"], ["provision", "--infra-only"], ["reconcile"],
                    ["reconcile", "--occupancy-id", owner, "--reset-generation", generation],
                    ["stop", "--execute"], ["occupy", "--occupancy-id", owner],
                    ["release", "--occupancy-id", owner, "--reset-generation", generation],
                    ["reset", "--execute"])
        with patch.object(b, "Azure", return_value=self.azure), patch.object(b, "preflight"), \
                contextlib.redirect_stderr(io.StringIO()):
            for command in commands:
                with self.subTest(command=command):
                    self.assertEqual(b.main([*command, "--env-file", str(env), "--output", str(self.output),
                                             "--tables", str(self.tables)]), 1)
        self.assertEqual(self.azure.metadata, before)
        self.assertEqual((self.azure.events, self.deployed, self.observed, self.child), ([], [], [], {}))
        self.assertFalse(self.output.exists())
        for bootstrap in (False, True):
            with self.assertRaisesRegex(b.Failure, "fenced, cleanup-owned"):
                self.control.admit(str(uuid.uuid4()), generation, bootstrap=bootstrap)
        self.assertEqual(self.azure.metadata, before)

class PublisherRegistryTests(unittest.TestCase):
    def metadata(self, entries):
        return {"publisherregistryversion": "1", "publishers": json.dumps(entries, separators=(",", ":"))}

    def test_explicit_versioned_empty_registry_is_drained(self):
        b.require_drained_publishers(self.metadata({}))

    def test_missing_unknown_malformed_and_duplicate_registry_fail_closed(self):
        for metadata in ({}, {"publisherregistryversion": "2", "publishers": "{}"},
                         {"publisherregistryversion": "1", "publishers": "["},
                         {"publisherregistryversion": "1", "publishers": '{"x":{},"x":{}}'},
                         self.metadata([])):
            with self.assertRaises(b.Failure):
                b.require_drained_publishers(metadata)

    def test_active_and_uncertain_never_expire_or_count_as_drained(self):
        for state in ("active", "uncertain"):
            metadata = self.metadata({str(uuid.uuid4()): {
                "generation": str(uuid.uuid4()), "state": state,
            }})
            with self.assertRaisesRegex(b.Failure, "block destructive"):
                b.require_drained_publishers(metadata)

    def test_bounded_slots_and_bytes_fail_closed(self):
        entries = {str(uuid.uuid4()): {"generation": str(uuid.uuid4()), "state": "active"}
                   for _ in range(17)}
        with self.assertRaises(b.Failure):
            b.publisher_registry(self.metadata(entries))
        with self.assertRaises(b.Failure):
            b.publisher_registry({"publisherregistryversion": "1", "publishers": " " * 4097})

    def test_unknown_state_extra_fields_and_invalid_ids_fail_closed(self):
        for key, entry in (
            ("invalid", {"generation": str(uuid.uuid4()), "state": "active"}),
            (str(uuid.uuid4()), {"generation": "bad", "state": "active"}),
            (str(uuid.uuid4()), {"generation": str(uuid.uuid4()), "state": "expired"}),
            (str(uuid.uuid4()), {"generation": str(uuid.uuid4()), "state": "active", "ttl": 1}),
        ):
            with self.assertRaises(b.Failure):
                b.publisher_registry(self.metadata({key: entry}))


if __name__ == "__main__":
    unittest.main()
