import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import bootstrap as b


SUB = "00000000-0000-0000-0000-000000000001"


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
        self.metadata = {"sandboxid": c.sandbox, "resetstate": "ready",
                         "resetgeneration": str(uuid.uuid4())}
        self.etag = '"one"'
        self.events = []

    def arm(self, method, path, **kwargs):
        if path.endswith("/blobServices/default"):
            return {"properties": self.settings}
        return {"tags": dict(self.tags), "properties": {"isHnsEnabled": True}}

    def collection(self, path, **kwargs):
        return self.databases if path.endswith("/databases") else self.containers

    def blobs(self, container):
        return list(self.items[container])

    def paths(self, container):
        return [{"name": name, "isDirectory": True} for name in self.directories[container]]

    def delete_directory(self, container, name):
        self.events.append(("DELETE_DIRECTORY", name))
        self.directories[container].remove(name)

    def control(self):
        return dict(self.metadata), self.etag

    def set_control(self, metadata, etag):
        if etag != self.etag:
            raise b.Failure("HTTP 412: metadata changed")
        self.metadata = dict(metadata)
        self.etag = str(uuid.uuid4())
        self.events.append(("control", metadata["resetstate"]))

    def cli(self, *args):
        self.events.append(("cli", args[2]))
        if args[:2] == ("rest", "--method") and args[2] == "delete":
            self.databases = []

    def blob(self, method, container, name, **kwargs):
        self.events.append((method, name))
        if method == "DELETE":
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

    def test_rejects_same_container(self):
        with self.assertRaises(b.Failure):
            b.Config({"AZURE_SUBSCRIPTION_ID": SUB, "AZURE_STORAGE_CHECKPOINT_CONTAINER": "arrowcol"})

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

    def tearDown(self):
        self.quiet.__exit__(None, None, None)

    def test_preview_has_no_mutations(self):
        b.reset(self.azure, False, None)
        self.assertEqual(self.azure.events, [])

    def test_execute_requires_actual_stop_command(self):
        with self.assertRaises(b.Failure):
            b.reset(self.azure, True, None)
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

    @patch.object(b.subprocess, "run")
    def test_stop_failure_retains_fence_without_deleting_data(self, run):
        run.return_value.returncode = 1
        with self.assertRaises(b.Failure):
            b.reset(self.azure, True, ["stop"])
        self.assertEqual(self.azure.events, [("control", "fenced")])
        self.assertEqual(self.azure.metadata["resetstate"], "fenced")

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_reset_fences_quiesces_then_deletes_every_blob_and_database(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        before = self.azure.metadata["resetgeneration"]
        b.reset(self.azure, True, ["stop"])
        sleep.assert_called_once_with(60)
        self.assertEqual(self.azure.events[0], ("control", "fenced"))
        self.assertIn(("DELETE", "table/data.parquet"), self.azure.events)
        self.assertNotEqual(self.azure.metadata["resetgeneration"], before)
        self.assertEqual(sum(len(items) for items in self.azure.items.values()), 0)
        self.assertEqual(self.azure.databases, [])
        self.assertEqual(self.azure.metadata["resetstate"], "fenced")
        self.assertFalse(any("fence.json" in event[1] for event in self.azure.events))
        b.reset(self.azure, False, None)

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_physical_emptiness_failure_is_visible(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        with patch.object(self.azure, "blob"):
            with self.assertRaisesRegex(b.Failure, "left workload"):
                b.reset(self.azure, True, ["stop"])
        self.assertEqual(self.azure.metadata["resetstate"], "fenced")

    @patch.object(b.subprocess, "run")
    def test_success_exit_without_verified_stop_receipt_is_not_enough(self, run):
        run.return_value.returncode = 0
        run.return_value.stdout = "not stopped"
        with self.assertRaisesRegex(b.Failure, "did not verify"):
            b.reset(self.azure, True, ["stop"])
        self.assertEqual(self.azure.events, [("control", "fenced")])

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_removes_empty_hns_directories_deepest_first(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        self.azure.directories[self.azure.c.data] = ["table", "table/empty", "table/empty/deeper"]
        b.reset(self.azure, True, ["stop"])
        self.assertEqual(self.azure.directories[self.azure.c.data], [])
        directories = [event[1] for event in self.azure.events if event[0] == "DELETE_DIRECTORY"]
        self.assertEqual(directories, ["table/empty/deeper", "table/empty", "table"])

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_late_inflight_publication_is_inventoried_after_grace(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        sleep.side_effect = lambda _: self.azure.items[self.azure.c.data].append({
            "name": "table/late.parquet", "version": None, "snapshot": None, "deleted": False,
        })
        b.reset(self.azure, True, ["stop"])
        self.assertIn(("DELETE", "table/late.parquet"), self.azure.events)

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_fence_change_during_quiescence_refuses_all_deletion(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        sleep.side_effect = lambda _: self.azure.metadata.update(resetgeneration=str(uuid.uuid4()))
        with self.assertRaisesRegex(b.Failure, "during quiescence"):
            b.reset(self.azure, True, ["stop"])
        self.assertFalse(any(event[0] in ("DELETE", "DELETE_DIRECTORY", "cli") for event in self.azure.events))


class OccupancyTests(unittest.TestCase):
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
            b.occupancy(azure, "release", owner)
        self.assertEqual(azure.metadata["occupancy"], "released")

    def test_fenced_sandbox_cannot_be_occupied(self):
        azure = FakeAzure()
        azure.metadata["resetstate"] = "fenced"
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
            with self.assertRaisesRegex(b.Failure, "root is missing"):
                b.reconcile(azure, manifest)

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


class StorageApiTests(unittest.TestCase):
    def setUp(self):
        self.azure = b.Azure(b.Config({"AZURE_SUBSCRIPTION_ID": SUB}))

    def test_container_metadata_read_and_conditional_update(self):
        c = self.azure.c
        generation = str(uuid.uuid4())
        with patch.object(self.azure, "blob") as blob:
            blob.return_value = (b"", {
                "x-ms-meta-sandboxid": c.sandbox, "x-ms-meta-resetstate": "ready",
                "x-ms-meta-resetgeneration": generation, "x-ms-meta-project": "rust-playground",
                "etag": '"generation-one"',
            })
            metadata, etag = self.azure.control()
            metadata["resetstate"] = "fenced"
            self.azure.set_control(metadata, etag)
            headers = blob.call_args.kwargs["headers"]
            self.assertEqual(headers["If-Match"], '"generation-one"')
            self.assertEqual(headers["x-ms-meta-resetgeneration"], generation)
            self.assertEqual(headers["x-ms-meta-project"], "rust-playground")
            self.assertEqual(headers["x-ms-meta-resetstate"], "fenced")

    def test_missing_ownership_state_generation_or_etag_fails_closed(self):
        c = self.azure.c
        complete = {"x-ms-meta-sandboxid": c.sandbox, "x-ms-meta-resetstate": "ready",
                    "x-ms-meta-resetgeneration": str(uuid.uuid4()), "etag": '"one"'}
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


if __name__ == "__main__":
    unittest.main()
