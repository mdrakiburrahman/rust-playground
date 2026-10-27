#!/usr/bin/env python3
"""Reconcile the isolated ArrowCol Azure sandbox using Azure CLI credentials."""

import argparse
import base64
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
TAGS = {"project": "rust-playground", "purpose": "disposable-arrowcol-sandbox"}


class Failure(RuntimeError):
    """A visible, sanitized operational failure."""


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
            "ARROWCOL_AZURE_TABLE_ROOT": f"abfss://{self.data}@{self.account}.dfs.core.windows.net/",
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
            **(headers or {}),
        })
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read(), {key.lower(): value for key, value in response.headers.items()}
        except urllib.error.HTTPError as exc:
            if missing_ok and exc.code == 404:
                return None, {}
            detail = exc.read().decode(errors="replace")
            raise Failure(f"HTTP {exc.code}: {self.c.redact(detail)}") from exc
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

    def control(self):
        _, headers = self.blob("HEAD", self.c.checkpoints, query={"restype": "container"})
        metadata = {key[10:]: value for key, value in headers.items()
                    if key.startswith("x-ms-meta-")}
        if metadata.get("sandboxid") != self.c.sandbox:
            raise Failure("Reset authority container ownership mismatch.")
        if metadata.get("resetstate") not in ("ready", "fenced"):
            raise Failure("Reset authority is missing or invalid; fail closed.")
        try:
            uuid.UUID(metadata["resetgeneration"])
        except (KeyError, ValueError) as exc:
            raise Failure("Reset authority generation is missing or invalid.") from exc
        if not headers.get("etag"):
            raise Failure("Reset authority ETag is unavailable.")
        return metadata, headers["etag"]

    def set_control(self, metadata, etag):
        self.blob("PUT", self.c.checkpoints,
                  query={"restype": "container", "comp": "metadata"}, body=b"",
                  headers={"If-Match": etag, **{"x-ms-meta-" + key: value
                                               for key, value in metadata.items()}})

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


def reconcile(azure, manifest):
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
        data, _ = azure.blob("GET", c.data, table["relative_path"] +
                             "/_delta_log/00000000000000000000.json",
                             missing_ok=True)
        if not data:
            raise Failure("Valid empty Delta root is missing; initialization must precede KQL.")
        actions = [json.loads(line) for line in data.splitlines() if line.strip()]
        if not any("metaData" in action for action in actions) or not any("protocol" in action for action in actions):
            raise Failure("Delta initialization did not publish metadata and protocol.")
    identity = azure.arm("GET", c.identity_id, api="2023-01-31")["properties"]["principalId"]
    policy = json.dumps([{"ObjectId": identity, "AllowedUsages": "ExternalTable,AutomatedFlows"}])
    azure.kql(f".alter-merge database ['{c.database}'] policy managed_identity '{policy}'")
    qap = json.dumps({"IsEnabled": True, "Hot": f"{c.hot}.00:00:00", "ManagedIdentity": identity})
    for table in tables:
        connection = (f"abfss://{c.data}@{c.account}.dfs.core.windows.net/{table['relative_path']}"
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
    c = azure.c
    databases, inventory = reset_inventory(azure)
    count = sum(len(items) for items in inventory.values())
    print(f"Reset preview: owned databases={len(databases)}, containers=2, stored objects={count}")
    print("Preserve cluster, account, containers and managed identity.")
    if not execute:
        return
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


def require_released_or_owner(metadata, owner):
    if metadata.get("occupancy", "released") == "released":
        return
    if not owner or metadata.get("occupancyid") != owner:
        raise Failure("Only the active cloud test owner may reconcile isolated table bindings.")


def occupancy(azure, action, owner):
    try:
        owner = str(uuid.UUID(owner))
    except ValueError as exc:
        raise Failure("Occupancy requires the runner's UUID --occupancy-id.") from exc
    metadata, etag = azure.control()
    if action == "occupy":
        if metadata["resetstate"] != "ready":
            raise Failure("Cannot start cloud tests while reset authority is fenced.")
        if metadata.get("occupancy", "released") != "released" and metadata.get("occupancyid") != owner:
            raise Failure("Another cloud test owns the sandbox.")
        metadata.update(occupancy="testing", occupancyid=owner)
    else:
        if metadata.get("occupancyid") != owner:
            raise Failure("Only the exact occupancy owner can release cloud tests.")
        metadata.update(occupancy="released")
    azure.set_control(metadata, etag)
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
    parser.add_argument("--stop-command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    c = None
    try:
        values = {**load_env(args.env_file), **os.environ}
        c = Config(values)
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
                reconcile(azure, args.tables)
                metadata["resetstate"] = "ready"
                azure.set_control(metadata, etag)
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
                require_released(azure.control()[0])
                azure.cli("rest", "--method", "post", "--url", "https://management.azure.com" +
                          c.cluster_id + "/stop?api-version=" + API)
                retry_ready(lambda: stopped(azure), "Cluster stop", timeout=1200)
                print("Owned Kusto cluster stopped; infrastructure preserved.")
            else:
                print("Stop preview: preserve cluster; --execute requests stop after test occupancy release.")
        elif args.command in ("occupy", "release"):
            occupancy(azure, args.command, args.occupancy_id)
        return 0
    except (Failure, ValueError, OSError, subprocess.TimeoutExpired) as exc:
        message = c.redact(str(exc)) if c else "Invalid or missing private Azure configuration."
        print("ERROR: " + message, file=sys.stderr)
        return 1


def stopped(azure):
    if azure.arm("GET", azure.c.cluster_id)["properties"].get("state") != "Stopped":
        raise Failure("Cluster is still stopping.")


if __name__ == "__main__":
    sys.exit(main())
