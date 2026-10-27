import contextlib
import io
import json
import os
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

    def head_control(self):
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

    def tearDown(self):
        self.quiet.__exit__(None, None, None)

    def test_preview_has_no_mutations(self):
        b.reset(self.azure, False, None)
        self.assertEqual(self.azure.events, [])

    def test_execute_requires_actual_stop_command(self):
        with self.assertRaises(b.Failure):
            b.reset(self.azure, True, None)
        self.assertEqual(self.azure.events, [])

    def test_public_execute_always_fails_closed_even_with_stop_receipt(self):
        with patch.object(b.subprocess, "run") as run:
            run.return_value.returncode = 0
            run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
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

    @patch.object(b.subprocess, "run")
    def test_stop_failure_retains_fence_without_deleting_data(self, run):
        run.return_value.returncode = 1
        with self.assertRaises(b.Failure):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.events, [("control", "fenced")])
        self.assertEqual(self.azure.metadata["resetstate"], "fenced")

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_reset_fences_quiesces_then_deletes_every_blob_and_database(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        before = self.azure.metadata["resetgeneration"]
        b._unqualified_reset_sequence(self.azure, ["stop"])
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
                b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.metadata["resetstate"], "fenced")

    @patch.object(b.subprocess, "run")
    def test_success_exit_without_verified_stop_receipt_is_not_enough(self, run):
        run.return_value.returncode = 0
        run.return_value.stdout = "not stopped"
        with self.assertRaisesRegex(b.Failure, "did not verify"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertEqual(self.azure.events, [("control", "fenced")])

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_removes_empty_hns_directories_deepest_first(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        self.azure.directories[self.azure.c.data] = ["table", "table/empty", "table/empty/deeper"]
        b._unqualified_reset_sequence(self.azure, ["stop"])
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
        b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertIn(("DELETE", "table/late.parquet"), self.azure.events)

    @patch.object(b.time, "sleep")
    @patch.object(b.subprocess, "run")
    def test_fence_change_during_quiescence_refuses_all_deletion(self, run, sleep):
        run.return_value.returncode = 0
        run.return_value.stdout = '{"stopped":true,"scope":"workspace-owned-loadtest","dataPreserved":true}\n'
        sleep.side_effect = lambda _: self.azure.metadata.update(resetgeneration=str(uuid.uuid4()))
        with self.assertRaisesRegex(b.Failure, "during quiescence"):
            b._unqualified_reset_sequence(self.azure, ["stop"])
        self.assertFalse(any(event[0] in ("DELETE", "DELETE_DIRECTORY", "cli") for event in self.azure.events))


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
            b.occupancy(azure, "release", owner)
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

    def test_unknown_arm_stop_outcome_keeps_hold_and_rerun_resumes(self):
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
        self.stop(azure)
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
        state = {"metadata": {"sandboxid": c.sandbox, "resetstate": "ready",
                              "resetgeneration": str(uuid.uuid4()), "project": "rust-playground",
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
        self.assertEqual(state["metadata"]["resetstate"], "ready")

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
        self.assertEqual(state["metadata"]["resetstate"], "ready")

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
    def test_existing_sandbox_is_fenced_before_any_deployment(self):
        azure = FakeAzure()
        c = azure.c
        existing = {
            "properties": {"metadata": {
                **azure.metadata, "project": "rust-playground",
            }}
        }
        with patch.object(b, "guard_existing"), \
                patch.object(azure, "arm", return_value=existing), \
                patch.object(azure, "cli", side_effect=b.Failure("stop before deployment")):
            with self.assertRaisesRegex(b.Failure, "stop before deployment"):
                b.deploy(azure)
        self.assertEqual(azure.events[0], ("control", "fenced"))
        self.assertEqual(azure.metadata["resetstate"], "fenced")

    def test_occupancy_acquired_after_arm_read_prevents_provision(self):
        azure = FakeAzure()
        existing = {"properties": {"metadata": dict(azure.metadata)}}
        azure.metadata["occupancy"] = "testing"
        with patch.object(b, "guard_existing"), \
                patch.object(azure, "arm", return_value=existing), \
                patch.object(azure, "cli") as cli:
            with self.assertRaisesRegex(b.Failure, "Cloud tests own"):
                b.deploy(azure)
            cli.assert_not_called()
        self.assertEqual(azure.events, [])


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
