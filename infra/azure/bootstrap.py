#!/usr/bin/env python3
"""Reconcile the isolated ArrowCol Azure sandbox using Azure CLI credentials."""

import argparse
import base64
from email.utils import formatdate
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
API = "2024-04-13"
# Short exclusive lease serializing authority metadata read-modify-write only.
LEASE_SECONDS = 15
TAGS = {"project": "rust-playground", "purpose": "disposable-arrowcol-sandbox"}
RESET_EXECUTION_BLOCKER = (
    "Destructive reset is disabled: complete distributed publisher registration and "
    "definitive request drain are not implemented and verified on every mutation surface. "
    "Local service stop, an assertion, and a timed grace period are not sufficient. "
    "Preview remains available; no resources or data were modified."
)


class Failure(RuntimeError):
    """A visible, sanitized operational failure."""


class HttpFailure(Failure):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


class ControlConflict(Failure):
    """An authority write was definitely not applied."""


class NotReady(Failure):
    """Only an absent declared Delta log, never an auth or ownership failure."""


def load_env(path):
    values = {}
    if not path.is_file():
        raise Failure("Private env file is missing; deliver it using repo-bootstrapper sync.")
    for number, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        if "=" not in line:
            raise Failure(f"Invalid env assignment on line {number}.")
        key, value = line.split("=", 1)
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise Failure(f"Invalid env key on line {number}.")
        if value[:1] in ("'", '"'):
            if len(value) < 2 or value[-1] != value[0]:
                raise Failure(f"Unclosed env quote on line {number}.")
            value = value[1:-1]
        # Never execute shell expansion or source private configuration as code.
        values[key] = value
    return values


class Config:
    def __init__(self, values):
        def get(key, default=""):
            return values.get(key) or default
        try:
            self.subscription = str(uuid.UUID(get("AZURE_SUBSCRIPTION_ID")))
        except ValueError as exc:
            raise Failure("AZURE_SUBSCRIPTION_ID must be a private subscription UUID.") from exc
        self.group = get("AZURE_RESOURCE_GROUP", "rust-playground")
        if self.group != "rust-playground":
            raise Failure("Only the exact rust-playground resource group is authorized.")
        self.sandbox = hashlib.sha256(
            (self.subscription + ":" + self.group).encode()
        ).hexdigest()[:12]
        self.location = get("AZURE_LOCATION", "eastus2")
        self.account = get("AZURE_STORAGE_ACCOUNT", "arrowcol" + self.sandbox)
        self.cluster = get("AZURE_KUSTO_CLUSTER", "arrowcol-" + self.sandbox)
        self.identity = get("AZURE_KUSTO_IDENTITY_NAME", "arrowcol-kusto-reader")
        self.database = get("AZURE_KUSTO_DATABASE", "arrowcol")
        self.data = get("AZURE_STORAGE_CONTAINER", "arrowcol")
        self.checkpoints = get("AZURE_STORAGE_CHECKPOINT_CONTAINER", "checkpoints")
        for value in (self.location, self.identity, self.cluster, self.database,
                      self.data, self.checkpoints):
            if not re.fullmatch(r"[a-z][a-z0-9-]{1,62}", value):
                raise Failure("Resource names must be lowercase alphanumeric sandbox names.")
        if not re.fullmatch(r"[a-z0-9]{3,24}", self.account):
            raise Failure("Invalid storage account name.")
        if self.data == self.checkpoints:
            raise Failure("Data and checkpoint containers must be distinct.")
        self.sku = get("AZURE_KUSTO_SKU", "Standard_E4ads_v5")
        self.minimum = int(get("AZURE_KUSTO_MIN_INSTANCES", "2"))
        self.maximum = int(get("AZURE_KUSTO_MAX_INSTANCES", "4"))
        self.hot = int(get("AZURE_KUSTO_QAP_HOT_DAYS", "7"))
        if not 2 <= self.minimum <= self.maximum <= 8 or not 1 <= self.hot <= 365:
            raise Failure("Require 2 <= cluster minimum <= maximum <= 8 and QAP days 1..365.")
        self.operator = get("AZURE_OPERATOR_OBJECT_ID")
        self.operator_type = get("AZURE_OPERATOR_TYPE", "User")
        self.operator_application = get("AZURE_OPERATOR_APPLICATION_ID")
        if self.operator_type not in ("User", "App"):
            raise Failure("AZURE_OPERATOR_TYPE must be User or App.")
        if self.operator_type == "App":
            try:
                self.operator_application = str(uuid.UUID(self.operator_application))
            except ValueError as exc:
                raise Failure("App operators require an explicit AZURE_OPERATOR_APPLICATION_ID.") from exc
        self.base = f"/subscriptions/{self.subscription}/resourceGroups/{self.group}"
        self.storage_id = self.base + "/providers/Microsoft.Storage/storageAccounts/" + self.account
        self.cluster_id = self.base + "/providers/Microsoft.Kusto/clusters/" + self.cluster
        self.identity_id = self.base + "/providers/Microsoft.ManagedIdentity/userAssignedIdentities/" + self.identity
        self.uri = f"https://{self.cluster}.{self.location}.kusto.windows.net"
        self.table_root = get("ARROWCOL_AZURE_TABLE_ROOT",
                              f"abfss://{self.data}@{self.account}.dfs.core.windows.net/")
        parsed = urllib.parse.urlsplit(self.table_root)
        if (parsed.scheme != "abfss" or
                parsed.netloc != f"{self.data}@{self.account}.dfs.core.windows.net" or
                parsed.query or parsed.fragment):
            raise Failure("Azure table root must stay inside the exact owned account and data container.")
        self.table_prefix = parsed.path.strip("/")
        if self.table_prefix and not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*",
                                                  self.table_prefix):
            raise Failure("Azure table root has an unsafe relative prefix.")
        self.table_root = f"abfss://{parsed.netloc}/" + (
            self.table_prefix + "/" if self.table_prefix else "")

    def redact(self, text):
        for value in (self.subscription, self.sandbox, self.account, self.cluster,
                      self.identity, self.uri, self.database, self.data, self.checkpoints):
            text = text.replace(value, "[private]")
        text = re.sub(r"https?://[^\s\"'<>]+", "[endpoint]", text)
        text = re.sub(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]+\b", "[principal]", text)
        text = re.sub(r"\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b",
                      "[identifier]", text)
        text = re.sub(r"(?i)bearer\s+\S+", "Bearer [redacted]", text)
        return text[:2000]

    def generated(self):
        return {
            "AZURE_LOCATION": self.location, "AZURE_STORAGE_ACCOUNT": self.account,
            "AZURE_STORAGE_CONTAINER": self.data,
            "AZURE_STORAGE_CHECKPOINT_CONTAINER": self.checkpoints,
            "AZURE_KUSTO_CLUSTER": self.cluster, "AZURE_KUSTO_DATABASE": self.database,
            "AZURE_KUSTO_IDENTITY_NAME": self.identity, "AZURE_KUSTO_URI": self.uri,
            "ARROWCOL_AZURE_TABLE_ROOT": self.table_root,
            "ARROWCOL_AZURE_CHECKPOINT_ROOT": f"abfss://{self.checkpoints}@{self.account}.dfs.core.windows.net/",
        }


def write_private(path, values):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".env.azure.")
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write("".join(f"{key}={value}\n" for key, value in values.items()))
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class Azure:
    def __init__(self, config):
        self.c = config
        self.tokens = {}
        self.snapshots = {}

    def cli(self, *args):
        result = subprocess.run(
            ["az", *args, "--only-show-errors", "--output", "json"],
            capture_output=True, text=True, timeout=3600, check=False,
        )
        if result.returncode:
            raise Failure(self.c.redact(result.stderr))
        return json.loads(result.stdout) if result.stdout.strip() else None

    def token(self, resource):
        cached = self.tokens.get(resource)
        if not cached or time.monotonic() - cached[0] > 240:
            token = self.cli("account", "get-access-token", "--subscription",
                             self.c.subscription, "--resource", resource)["accessToken"]
            self.tokens[resource] = (time.monotonic(), token)
        return self.tokens[resource][1]

    def request(self, method, url, resource, body=None, headers=None, missing_ok=False):
        encoded = body.encode() if isinstance(body, str) else body
        request = urllib.request.Request(url, encoded, method=method, headers={
            "Authorization": "Bearer " + self.token(resource),
            **({"x-ms-date": formatdate(usegmt=True)}
               if resource == "https://storage.azure.com/" else {}),
            **(headers or {}),
        })
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read(), {key.lower(): value for key, value in response.headers.items()}
        except urllib.error.HTTPError as exc:
            if missing_ok and exc.code == 404:
                return None, {}
            detail = exc.read().decode(errors="replace")
            raise HttpFailure(exc.code, f"HTTP {exc.code}: {self.c.redact(detail)}") from exc
        except urllib.error.URLError as exc:
            raise Failure(self.c.redact(str(exc.reason))) from exc

    def arm(self, method, path, body=None, missing_ok=False, api=API):
        data, _ = self.request(method, "https://management.azure.com" + path +
                               ("&" if "?" in path else "?") + "api-version=" + api,
                               "https://management.azure.com/",
                               json.dumps(body) if body is not None else None,
                               {"Content-Type": "application/json"}, missing_ok)
        return json.loads(data) if data else None

    def blob(self, method, container, name="", query=None, body=None, headers=None,
             missing_ok=False):
        url = f"https://{self.c.account}.blob.core.windows.net/{container}"
        if name:
            url += "/" + urllib.parse.quote(name, safe="/")
        if query:
            url += "?" + urllib.parse.urlencode(query)
        return self.request(method, url, "https://storage.azure.com/", body, {
            "x-ms-version": "2023-11-03", **(headers or {})
        }, missing_ok)

    def blobs(self, container):
        entries, marker = [], ""
        while True:
            data, _ = self.blob("GET", container, query={
                "restype": "container", "comp": "list",
                "include": "snapshots,versions,deleted,metadata", "marker": marker,
            })
            tree = ET.fromstring(data)
            for entry in tree.findall("./Blobs/Blob"):
                entries.append({
                    "name": entry.findtext("Name"),
                    "snapshot": entry.findtext("Snapshot"),
                    "version": entry.findtext("VersionId"),
                    "deleted": entry.findtext("Deleted") == "true",
                })
            marker = tree.findtext("NextMarker")
            if not marker:
                return entries

    def paths(self, container):
        paths, continuation = [], ""
        while True:
            query = urllib.parse.urlencode({
                "resource": "filesystem", "recursive": "true", "maxResults": "5000",
                "continuation": continuation,
            })
            data, headers = self.request(
                "GET", f"https://{self.c.account}.dfs.core.windows.net/{container}?{query}",
                "https://storage.azure.com/", headers={"x-ms-version": "2023-11-03"},
            )
            paths.extend(json.loads(data).get("paths", []))
            continuation = headers.get("x-ms-continuation")
            if not continuation:
                return paths

    def delete_directory(self, container, name):
        path = urllib.parse.quote(name, safe="/")
        self.request(
            "DELETE", f"https://{self.c.account}.dfs.core.windows.net/{container}/{path}?recursive=false",
            "https://storage.azure.com/", headers={"x-ms-version": "2023-11-03"},
            missing_ok=True,
        )

    def kql(self, command, database=None):
        data, _ = self.request(
            "POST", self.c.uri + "/v1/rest/mgmt", "https://kusto.kusto.windows.net",
            json.dumps({"db": database or self.c.database, "csl": command}),
            {"Content-Type": "application/json"},
        )
        value = json.loads(data)
        if value.get("error") or value.get("HasErrors") or value.get("Exceptions"):
            raise Failure(self.c.redact(json.dumps(value)))
        return value

    def head_control(self):
        """Read authority metadata and remember its content for a later guarded write."""
        _, headers = self.blob("HEAD", self.c.checkpoints, query={"restype": "container"})
        metadata = {key[10:].lower(): value for key, value in headers.items()
                    if key.startswith("x-ms-meta-")}
        if not headers.get("etag"):
            raise Failure("Reset authority ETag is unavailable.")
        self.snapshots[headers["etag"]] = dict(metadata)
        return metadata, headers["etag"]

    def control(self):
        metadata, etag = self.head_control()
        if metadata.get("sandboxid") != self.c.sandbox:
            raise Failure("Reset authority container ownership mismatch.")
        if metadata.get("resetstate") not in ("ready", "fenced"):
            raise Failure("Reset authority is missing or invalid; fail closed.")
        try:
            uuid.UUID(metadata["resetgeneration"])
        except (KeyError, ValueError) as exc:
            raise Failure("Reset authority generation is missing or invalid.") from exc
        return metadata, etag

    def lease(self, action, lease_id, proposed=False):
        headers = {"x-ms-lease-action": action}
        if proposed:
            headers.update({"x-ms-lease-duration": str(LEASE_SECONDS),
                            "x-ms-proposed-lease-id": lease_id})
        else:
            headers["x-ms-lease-id"] = lease_id
        self.blob("PUT", self.c.checkpoints, query={"comp": "lease", "restype": "container"},
                  body=b"", headers=headers)

    def set_control(self, metadata, etag):
        """Compare-and-set authority metadata under an exclusive container lease.

        Set Container Metadata ignores If-Match, so every protocol writer holds a
        short container lease, re-reads, compares content with the caller's read,
        then writes ALL metadata with x-ms-lease-id. A conflict, busy lease or 412
        is definitely not applied. Other failures may have applied remotely.
        """
        expected = self.snapshots.get(etag)
        if expected is None:
            raise ControlConflict("Authority write has no matching prior read; nothing written.")
        lease_id = str(uuid.uuid4())
        try:
            self.lease("acquire", lease_id, proposed=True)
        except Failure as exc:
            # 409 busy or an ambiguous acquire: no metadata write is attempted, and
            # any lease we may hold expires within LEASE_SECONDS.
            raise ControlConflict("Authority lease unavailable; nothing written.") from exc
        try:
            current, _ = self.head_control()
            if current != expected:
                raise ControlConflict("Authority changed since it was read; nothing written.")
            try:
                self.blob("PUT", self.c.checkpoints,
                          query={"restype": "container", "comp": "metadata"}, body=b"",
                          headers={"x-ms-lease-id": lease_id,
                                   **{"x-ms-meta-" + key: value for key, value in metadata.items()}})
            except HttpFailure as exc:
                if exc.code == 412:
                    raise ControlConflict("Authority lease lost before write; nothing written.") from exc
                raise
        finally:
            try:
                self.lease("release", lease_id)
            except Failure:
                pass  # Expiry bounds an unreleased lease.
    def collection(self, path, api=API):
        page = self.arm("GET", path, api=api)
        items = page["value"]
        while page.get("nextLink"):
            link = page["nextLink"]
            if not link.startswith("https://management.azure.com" + path + "?"):
                raise Failure("Unexpected ARM pagination endpoint; refusing unsafe inventory.")
            data, _ = self.request("GET", link, "https://management.azure.com/")
            page = json.loads(data)
            items.extend(page["value"])
        return items


def owned(resource, config):
    if not resource or resource.get("tags", {}) != {**TAGS, "sandboxId": config.sandbox}:
        raise Failure("Exact sandbox ownership tags do not match; refusing adoption/mutation.")


def retry_ready(action, label, timeout=600):
    deadline, last = time.monotonic() + timeout, None
    while True:
        try:
            return action()
        except Failure as exc:
            last = exc
        if time.monotonic() >= deadline:
            raise Failure(f"{label} readiness timed out: {last}") from last
        time.sleep(10)


def preflight(azure):
    c = azure.c
    account = azure.cli("account", "show", "--subscription", c.subscription)
    if account["id"].lower() != c.subscription:
        raise Failure("CLI subscription mismatch; no default-subscription fallback is permitted.")
    for provider in ("Microsoft.Kusto", "Microsoft.Storage", "Microsoft.ManagedIdentity"):
        state = azure.cli("provider", "show", "--namespace", provider,
                          "--subscription", c.subscription)["registrationState"]
        if state != "Registered":
            azure.cli("provider", "register", "--namespace", provider,
                      "--subscription", c.subscription, "--wait")
    if not c.operator:
        token = azure.token("https://management.azure.com/")
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        c.operator = str(uuid.UUID(claims["oid"]))
    else:
        c.operator = str(uuid.UUID(c.operator))
    print("Azure subscription and signed-in provisioning identity: ready")


def guard_existing(azure):
    c = azure.c
    for path in (c.base, c.storage_id, c.cluster_id, c.identity_id):
        resource = azure.arm("GET", path, missing_ok=True,
                             api="2022-09-01" if path == c.base else
                             "2023-05-01" if path == c.storage_id else
                             "2023-01-31" if path == c.identity_id else API)
        if resource:
            owned(resource, c)


def deploy(azure):
    c = azure.c
    guard_existing(azure)
    container = azure.arm("GET", c.storage_id + "/blobServices/default/containers/" +
                          c.checkpoints, missing_ok=True, api="2023-05-01")
    control = (container["properties"]["metadata"] if container else
               {"resetstate": "fenced", "resetgeneration": str(uuid.uuid4())})
    if container and (control.get("sandboxid") != c.sandbox or
                      control.get("resetstate") not in ("ready", "fenced")):
        raise Failure("Existing checkpoint container has no valid reset authority.")
    if control.get("occupancy") == "testing":
        raise Failure("Cloud tests own the sandbox; provisioning requires their explicit release.")
    if container:
        # Fence with a data-plane compare/exchange before ARM work. Otherwise a
        # test can acquire occupancy between the preliminary read and deployment.
        metadata, etag = azure.control()
        require_released(metadata)
        metadata.update(resetstate="fenced", resetgeneration=str(uuid.uuid4()))
        azure.set_control(metadata, etag)
        control = metadata
    params = {
        "location": c.location, "resourceGroupName": c.group,
        "sandboxId": c.sandbox, "storageAccountName": c.account,
        "clusterName": c.cluster, "identityName": c.identity,
        "databaseName": c.database, "dataContainer": c.data,
        "checkpointContainer": c.checkpoints, "operatorObjectId": c.operator,
        "operatorKustoPrincipalId": c.operator_application if c.operator_type == "App" else c.operator,
        "controlMetadata": control,
        "checkpointExists": container is not None,
        "operatorType": c.operator_type, "clusterSku": c.sku,
        "minimumInstances": c.minimum, "maximumInstances": c.maximum,
    }
    fd, name = tempfile.mkstemp(prefix=".env.azure.parameters.", dir=ROOT)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump({"parameters": {k: {"value": v} for k, v in params.items()}}, stream)
        os.chmod(name, 0o600)
        azure.cli("deployment", "sub", "create", "--subscription", c.subscription,
                  "--location", c.location, "--name", "arrowcol-sandbox",
                  "--template-file", str(Path(__file__).with_name("main.bicep")),
                  "--parameters", "@" + name)
    finally:
        os.unlink(name)
    cluster = azure.arm("GET", c.cluster_id)
    c.uri = cluster["properties"]["uri"].rstrip("/")
    if cluster["properties"].get("state") == "Stopped":
        azure.cli("rest", "--method", "post", "--url",
                  "https://management.azure.com" + c.cluster_id + "/start?api-version=" + API)
    retry_ready(lambda: azure.kql(".show cluster"), "Kusto")
    retry_ready(lambda: azure.blobs(c.data), "Storage RBAC")
    print("Declarative resources, Kusto and storage access: ready")


def reconcile(azure, manifest, wait_seconds=0):
    c = azure.c
    tables = json.loads(manifest.read_text())
    if not isinstance(tables, list) or not tables:
        raise Failure("Initialization manifest must be a nonempty array of table bindings.")
    tables = [{"name": table, "relative_path": table} if isinstance(table, str)
              else table for table in tables]
    if any(not isinstance(table, dict) or set(table) != {"name", "relative_path"} for table in tables):
        raise Failure("Each table binding must contain exactly name and relative_path.")
    if any(not isinstance(table["name"], str) or not isinstance(table["relative_path"], str)
           for table in tables):
        raise Failure("Table names and relative paths must be strings.")
    if len({table["name"] for table in tables}) != len(tables):
        raise Failure("Initialization manifest has duplicate tables.")
    for table in tables:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", table["name"]):
            raise Failure("Initialization manifest contains an invalid table name.")
        if not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", table["relative_path"]):
            raise Failure("Initialization manifest contains an unsafe relative path.")
        relative = "/".join(part for part in (c.table_prefix, table["relative_path"]) if part)
        table["storage_path"] = relative
    deadline = time.monotonic() + wait_seconds
    while True:
        missing = 0
        for table in tables:
            data, _ = azure.blob("GET", c.data, table["storage_path"] +
                                 "/_delta_log/00000000000000000000.json",
                                 missing_ok=True)
            if data is None:
                missing += 1
                continue
            try:
                actions = [json.loads(line) for line in data.splitlines() if line.strip()]
            except json.JSONDecodeError as exc:
                raise Failure("Delta initialization log is malformed.") from exc
            if not any("metaData" in action for action in actions) or not any("protocol" in action for action in actions):
                raise Failure("Delta initialization did not publish metadata and protocol.")
        if not missing:
            break
        if time.monotonic() >= deadline:
            raise NotReady(f"Declared Delta logs are absent: {missing}; no KQL was attempted.")
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    identity = azure.arm("GET", c.identity_id, api="2023-01-31")["properties"]["principalId"]
    policy = json.dumps([{"ObjectId": identity, "AllowedUsages": "ExternalTable,AutomatedFlows"}])
    azure.kql(f".alter-merge database ['{c.database}'] policy managed_identity '{policy}'")
    qap = json.dumps({"IsEnabled": True, "Hot": f"{c.hot}.00:00:00", "ManagedIdentity": identity})
    for table in tables:
        connection = (f"abfss://{c.data}@{c.account}.dfs.core.windows.net/{table['storage_path']}"
                      f";managed_identity={identity}")
        retry_ready(lambda: azure.kql(
            f".create-or-alter external table ['{table['name']}'] kind=delta (h@'{connection}')"
        ), "External Delta table")
        azure.kql(f".alter external table ['{table['name']}'] policy query_acceleration '{qap}'")
    print(f"External Delta tables and QAP reconciled: {len(tables)}")


def reset_inventory(azure):
    c = azure.c
    guard_existing(azure)
    owned(azure.arm("GET", c.cluster_id), c)
    storage = azure.arm("GET", c.storage_id, api="2023-05-01")
    owned(storage, c)
    if not storage["properties"].get("isHnsEnabled"):
        raise Failure("Reset requires the declared HNS storage account.")
    service = azure.arm("GET", c.storage_id + "/blobServices/default", api="2023-05-01")["properties"]
    if (service.get("isVersioningEnabled") or
            service.get("deleteRetentionPolicy", {}).get("enabled") or
            service.get("containerDeleteRetentionPolicy", {}).get("enabled") or
            service.get("restorePolicy", {}).get("enabled")):
        raise Failure("Retained copies are enabled; refusing a reset that cannot guarantee deletion.")
    databases = azure.collection(c.cluster_id + "/databases")
    if any(item["name"].split("/")[-1] != c.database for item in databases):
        raise Failure("Unexpected Kusto database present; refusing shared-resource reset.")
    containers = azure.collection(c.storage_id + "/blobServices/default/containers",
                                  api="2023-05-01")
    if {item["name"].split("/")[-1] for item in containers} != {c.data, c.checkpoints}:
        raise Failure("Unexpected or missing container; refusing shared-resource reset.")
    for container in containers:
        if container["properties"].get("metadata", {}).get("sandboxid") != c.sandbox:
            raise Failure("Container ownership metadata mismatch.")
    inventory = {name: azure.blobs(name) for name in (c.data, c.checkpoints)}
    for name in (c.data, c.checkpoints):
        by_name = {item["name"]: item for item in inventory[name]}
        for path in azure.paths(name):
            entry = by_name.get(path["name"])
            directory = str(path.get("isDirectory", False)).lower() == "true"
            if entry is not None:
                entry["directory"] = directory
            else:
                inventory[name].append({
                    "name": path["name"], "directory": directory,
                    "deleted": False, "version": None, "snapshot": None,
                })
    if any(blob["deleted"] for blobs in inventory.values() for blob in blobs):
        raise Failure("Soft-deleted objects remain; reset cannot truthfully claim physical emptiness.")
    for entries in inventory.values():
        for entry in entries:
            name = entry["name"]
            if (not isinstance(name, str) or not name or name.startswith("/") or
                    "\\" in name or any(part in (".", "..") for part in name.split("/"))):
                raise Failure("Storage inventory contains a malformed or unscoped path.")
    return databases, inventory


def reset(azure, execute, stop_command):
    if execute:
        raise Failure(RESET_EXECUTION_BLOCKER)
    return reset_preview(azure)


def reset_preview(azure):
    databases, inventory = reset_inventory(azure)
    count = sum(len(items) for items in inventory.values())
    print(f"Reset preview: owned databases={len(databases)}, containers=2, stored objects={count}")
    print("Preserve cluster, account, containers and managed identity.")


def _unqualified_reset_sequence(azure, stop_command):
    """Non-dispatched sequence; not executable until distributed drain is qualified."""
    c = azure.c
    databases, inventory = reset_inventory(azure)
    count = sum(len(items) for items in inventory.values())
    print(f"Reset preview: owned databases={len(databases)}, containers=2, stored objects={count}")
    print("Preserve cluster, account, containers and managed identity.")
    if not stop_command:
        raise Failure("Execution requires the runner's --stop-command to stop local writers/consumers.")
    metadata, etag = azure.control()
    require_released(metadata)
    metadata.update(resetstate="fenced", resetgeneration=str(uuid.uuid4()))
    azure.set_control(metadata, etag)
    result = subprocess.run(stop_command, cwd=ROOT, timeout=180, check=False,
                            capture_output=True, text=True)
    if result.returncode:
        raise Failure("Writer/consumer stop failed; reset fence retained, no workload data deleted. " +
                      c.redact(str(result.stderr)))
    receipts = []
    for line in result.stdout.splitlines():
        try:
            receipts.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if not any(isinstance(receipt, dict) and receipt.get("stopped") is True and
               receipt.get("scope") == "workspace-owned-loadtest" and
               receipt.get("dataPreserved") is True for receipt in receipts):
        raise Failure("Runner did not verify workspace-owned services stopped; reset fence retained.")
    print("Distributed reset fence active; waiting 60s for bounded in-flight publications.")
    time.sleep(60)
    databases, inventory = reset_inventory(azure)
    if azure.control()[0] != metadata:
        raise Failure("Reset authority changed during quiescence; refusing data deletion.")
    if databases:
        azure.cli("rest", "--method", "delete", "--url", "https://management.azure.com" +
                  c.cluster_id + "/databases/" + c.database + "?api-version=" + API)
    for container, blobs in inventory.items():
        for blob in sorted(blobs, key=lambda item: item["name"].count("/"), reverse=True):
            if blob.get("directory"):
                azure.delete_directory(container, blob["name"])
                continue
            query = {}
            if blob["version"]:
                query["versionid"] = blob["version"]
            elif blob["snapshot"]:
                query["snapshot"] = blob["snapshot"]
            azure.blob("DELETE", container, blob["name"], query=query,
                       headers={} if query else {"x-ms-delete-snapshots": "include"},
                       missing_ok=True)
    remaining = [b for name in (c.data, c.checkpoints) for b in azure.blobs(name)]
    remaining.extend(path for name in (c.data, c.checkpoints) for path in azure.paths(name))
    if remaining:
        raise Failure("Reset left workload state behind; fence retained.")
    retry_ready(lambda: no_databases(azure), "Database deletion")
    if azure.control()[0] != metadata:
        raise Failure("Reset authority changed during deletion; reset is not verified.")
    print("Reset verified: zero databases and zero blobs, versions or snapshots; container metadata remains fenced.")


def no_databases(azure):
    if azure.collection(azure.c.cluster_id + "/databases"):
        raise Failure("Database deletion is still in progress.")


def require_released(metadata):
    if metadata.get("occupancy", "released") != "released":
        raise Failure("Cloud tests own the sandbox; wait for explicit occupancy release.")


def publisher_registry(metadata):
    """Validate shared wire format without inferring completion from missing data."""
    if metadata.get("publisherregistryversion") != "1":
        raise Failure("Publisher registry is missing or has an unsupported version.")
    raw = metadata.get("publishers")
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > 4096:
        raise Failure("Publisher registry exceeds its bounded metadata budget or is missing.")
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise Failure("Publisher registry contains duplicate JSON keys.")
            result[key] = value
        return result
    try:
        registry = json.loads(raw, object_pairs_hook=unique_pairs)
    except json.JSONDecodeError as exc:
        raise Failure("Publisher registry JSON is malformed.") from exc
    if not isinstance(registry, dict) or len(registry) > 16:
        raise Failure("Publisher registry must be a map with at most 16 slots.")
    for publisher, entry in registry.items():
        try:
            if str(uuid.UUID(publisher)) != publisher:
                raise ValueError("noncanonical publisher")
            if not isinstance(entry, dict) or set(entry) != {"generation", "state"}:
                raise ValueError("invalid entry")
            if str(uuid.UUID(entry["generation"])) != entry["generation"]:
                raise ValueError("noncanonical generation")
            if entry["state"] not in ("active", "uncertain"):
                raise ValueError("invalid state")
        except (ValueError, TypeError, AttributeError) as exc:
            raise Failure("Publisher registry contains an invalid publisher entry.") from exc
    return registry


def require_drained_publishers(metadata):
    if publisher_registry(metadata):
        raise Failure("Registered active or uncertain publishers block destructive cleanup; no expiry or force-clear is permitted.")


def require_released_or_owner(metadata, owner):
    if metadata.get("occupancy", "released") == "released":
        return
    if not owner or metadata.get("occupancyid") != owner:
        raise Failure("Only the active cloud test owner may reconcile isolated table bindings.")


def occupancy(azure, action, owner, output=None):
    try:
        owner = str(uuid.UUID(owner))
    except ValueError as exc:
        raise Failure("Occupancy requires the runner's UUID --occupancy-id.") from exc
    metadata, etag = azure.control()
    current = metadata.get("occupancy", "released")
    if action == "occupy":
        if metadata["resetstate"] != "ready":
            raise Failure("Cannot start cloud tests while reset authority is fenced.")
        if current == "stopping":
            raise Failure("A cluster stop holds the sandbox; tests cannot start.")
        if current != "released" and (current != "testing" or metadata.get("occupancyid") != owner):
            raise Failure("Another cloud test owns the sandbox.")
        metadata.update(occupancy="testing", occupancyid=owner)
    else:
        if current != "testing" or metadata.get("occupancyid") != owner:
            raise Failure("Only the exact occupancy owner can release cloud tests.")
        metadata.update(occupancy="released")
    azure.set_control(metadata, etag)
    if action == "occupy" and output is not None:
        # Re-read after CAS: never infer generation from the occupancy/run UUID.
        current, _ = azure.control()
        if (current.get("occupancyid") != owner or current.get("occupancy") != "testing" or
                current.get("resetstate") != "ready" or
                current.get("resetgeneration") != metadata["resetgeneration"]):
            raise Failure("Authority changed after occupancy; do not admit collectors.")
        values = azure.c.generated()
        values["ARROWCOL_RESET_GENERATION"] = current["resetgeneration"]
        write_private(output, values)
    print("Cloud test occupancy: " + ("TESTING" if action == "occupy" else "RELEASED"))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("config", "preflight", "provision", "reconcile", "reset", "status", "stop", "occupy", "release"))
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--output", type=Path, default=ROOT / ".env.azure")
    parser.add_argument("--tables", type=Path, default=ROOT / ".env.azure.tables.json")
    parser.add_argument("--infra-only", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--occupancy-id", default="")
    parser.add_argument("--wait-seconds", type=int, default=0)
    parser.add_argument("--stop-command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    c = None
    try:
        values = {**load_env(args.env_file), **os.environ}
        c = Config(values)
        if not 0 <= args.wait_seconds <= 600:
            raise Failure("--wait-seconds must be in the range 0..600.")
        azure = Azure(c)
        if args.command == "config":
            write_private(args.output, c.generated())
            print("Private generated Azure configuration written (values withheld).")
            return 0
        if args.command == "preflight":
            preflight(azure)
        elif args.command == "provision":
            preflight(azure)
            deploy(azure)
            write_private(args.output, c.generated())
            if not args.infra_only:
                metadata, etag = azure.control()
                require_released(metadata)
                metadata.update(resetstate="fenced", resetgeneration=str(uuid.uuid4()))
                azure.set_control(metadata, etag)
                metadata, etag = azure.control()
                # The foundation initializer owns schema/protocol correctness.
                initializer = ROOT / "infra/azure/initialize-delta.sh"
                if not initializer.is_file():
                    raise Failure("Delta initializer is unavailable; infra exists, full bootstrap is NOT complete.")
                result = subprocess.run(["bash", str(initializer)], cwd=ROOT, timeout=1800,
                                        env={**os.environ, **values, **c.generated(),
                                             "ARROWCOL_AZURE_TABLE_MANIFEST": str(args.tables.resolve())},
                                        check=False,
                                        capture_output=True, text=True)
                if result.returncode:
                    raise Failure("Delta initialization failed; KQL reconciliation was not attempted. " +
                                  c.redact(result.stderr))
                reconcile(azure, args.tables, args.wait_seconds)
                metadata["resetstate"] = "ready"
                azure.set_control(metadata, etag)
                write_private(args.output, {
                    **c.generated(), "ARROWCOL_RESET_GENERATION": metadata["resetgeneration"],
                })
                print("Full bootstrap ready; reset fence cleared.")
            else:
                print("INFRA-ONLY: Delta initialization and external-table/QAP reconciliation NOT verified.")
        elif args.command == "reconcile":
            require_released_or_owner(azure.control()[0], args.occupancy_id)
            reconcile(azure, args.tables)
        elif args.command == "reset":
            reset(azure, args.execute, args.stop_command or [
                "npx", "--no-install", "tsx", "projects/loadtest/src/main.ts", "--stop",
            ])
        elif args.command == "status":
            reset_inventory(azure)
            azure.kql(".show capacity")
            print("Owned infrastructure and Kusto capacity: responsive")
        elif args.command == "stop":
            owned(azure.arm("GET", c.cluster_id), c)
            if args.execute:
                stop_cluster(azure)
                print("Owned Kusto cluster stopped; infrastructure preserved.")
            else:
                print("Stop preview: preserve cluster; --execute requests stop after test occupancy release.")
        elif args.command in ("occupy", "release"):
            occupancy(azure, args.command, args.occupancy_id,
                      args.output if args.command == "occupy" else None)
        return 0
    except NotReady:
        print(json.dumps({"status": "not_ready", "code": "DELTA_LOGS_ABSENT",
                          "retryable": True, "kql_attempted": False}), file=sys.stderr)
        return 3
    except (Failure, ValueError, OSError, subprocess.TimeoutExpired) as exc:
        message = c.redact(str(exc)) if c else "Invalid or missing private Azure configuration."
        print("ERROR: " + message, file=sys.stderr)
        return 1


def stop_cluster(azure):
    """Serialize stop with occupy through a persistent occupancy=stopping hold.

    The hold is taken by CAS from released before any ARM call, so a competing
    occupy either wins first (stop fails before ARM) or is rejected afterwards.
    It is released only after Stopped is observed. Any unknown CAS or ARM
    outcome leaves the hold in place; rerunning stop resumes the same hold.
    """
    metadata, etag = azure.control()
    current = metadata.get("occupancy", "released")
    if current == "released":
        metadata.update(occupancy="stopping", occupancyid=str(uuid.uuid4()))
        azure.set_control(metadata, etag)
    elif current != "stopping":
        raise Failure("Cloud tests own the sandbox; wait for explicit occupancy release.")
    azure.cli("rest", "--method", "post", "--url", "https://management.azure.com" +
              azure.c.cluster_id + "/stop?api-version=" + API)
    retry_ready(lambda: stopped(azure), "Cluster stop", timeout=1200)
    metadata, etag = azure.control()
    if metadata.get("occupancy") != "stopping":
        raise Failure("Stop hold changed unexpectedly; reconcile occupancy before reuse.")
    metadata["occupancy"] = "released"
    azure.set_control(metadata, etag)


def stopped(azure):
    if azure.arm("GET", azure.c.cluster_id)["properties"].get("state") != "Stopped":
        raise Failure("Cluster is still stopping.")


if __name__ == "__main__":
    sys.exit(main())
