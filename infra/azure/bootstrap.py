#!/usr/bin/env python3
"""Reconcile the isolated ArrowCol Azure sandbox using Azure CLI credentials."""

import argparse
import base64
import contextlib
from email.utils import formatdate
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

if __name__ == "__main__":
    # publisher_control imports from "bootstrap"; share this module's classes.
    sys.modules.setdefault("bootstrap", sys.modules[__name__])

ROOT = Path(__file__).resolve().parents[2]
API = "2024-04-13"
# Short exclusive lease serializing authority metadata read-modify-write only.
LEASE_SECONDS = 15
TAGS = {"project": "rust-playground", "purpose": "disposable-arrowcol-sandbox"}
DEFAULT_STOP_COMMAND = ["npx", "--no-install", "tsx", "projects/loadtest/src/main.ts", "--stop"]


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


class DeploymentFailed(Failure):
    """ARM reported a terminal Failed deployment: definite, with no data-plane I/O."""


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


RESERVED_TAGS = {key.lower() for key in (*TAGS, "sandboxId")}


def cluster_tags(raw):
    """Optional extra Kusto cluster tags required by tenant policy; never ownership keys."""
    try:
        tags = json.loads(raw)
    except ValueError as exc:
        raise Failure("AZURE_KUSTO_CLUSTER_TAGS must be a JSON object of string tags.") from exc
    if (not isinstance(tags, dict) or len(tags) > 15 or
            any(not isinstance(key, str) or not isinstance(value, str) or
                not 0 < len(key) <= 512 or len(value) > 256 or key.lower() in RESERVED_TAGS
                for key, value in tags.items())):
        raise Failure("AZURE_KUSTO_CLUSTER_TAGS must be at most 15 string tags that do not "
                      "override sandbox ownership tags.")
    return tags


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
        self.cluster_tags = cluster_tags(get("AZURE_KUSTO_CLUSTER_TAGS", "{}"))
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


def run_tree(command, timeout, cwd=None, env=None):
    """Run a command in its own session; on timeout kill the whole process group.

    A timeout leaves the outcome unknown, so callers must not treat it as a
    definite failure of a mutating request.
    """
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, start_new_session=True, cwd=cwd, env=env)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except BaseException as exc:  # timeout or outer interruption: tear down the whole tree
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate()
        if not isinstance(exc, subprocess.TimeoutExpired):
            raise
        raise Failure("Command timed out; outcome unknown and process group killed. "
                      "Authority stays blocking; no automatic clear.")
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)

class Azure:
    def __init__(self, config):
        self.c = config
        self.tokens = {}
        self.snapshots = {}

    def cli(self, *args):
        result = run_tree(["az", *args, "--only-show-errors", "--output", "json"], 3600)
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
                return response.read(), {**{key.lower(): value for key, value in response.headers.items()},
                                         ":status": str(response.status)}
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

    def delete_database(self):
        """Submit the owned database DELETE; return (HTTP status, provider operation URL)."""
        _, headers = self.request(
            "DELETE", "https://management.azure.com" + self.c.cluster_id + "/databases/" +
            self.c.database + "?api-version=" + API, "https://management.azure.com/", missing_ok=True)
        if not headers:
            return 404, None
        # Only Azure-AsyncOperation carries an explicit status contract; Location-only is unknown.
        return int(headers[":status"]), headers.get("azure-asyncoperation")

    def provider_operation(self, url):
        """Read-only Azure-AsyncOperation poll: Succeeded, Failed, Canceled, InProgress or Unknown.

        Anything without an explicit known status is Unknown, never a default success.
        """
        url = operation_url(url)  # validated before any token is fetched
        data, headers = self.request("GET", url, "https://management.azure.com/")
        if headers.get(":status") != "200":
            return "Unknown"
        try:
            body = json.loads(data)
        except (TypeError, ValueError):
            return "Unknown"
        status = body.get("status") if isinstance(body, dict) else None
        if not isinstance(status, str):
            return "Unknown"
        return PROVIDER_STATUSES.get(status.lower(), "Unknown")

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
                    "etag": entry.findtext("Properties/Etag"),
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

    def delete_directory(self, container, name, etag):
        path = urllib.parse.quote(name, safe="/")
        self.request(
            "DELETE", f"https://{self.c.account}.dfs.core.windows.net/{container}/{path}?recursive=false",
            "https://storage.azure.com/", headers={"x-ms-version": "2023-11-03", "If-Match": etag},
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
        """Validated v1 authority only; legacy/unversioned metadata is refused."""
        from publisher_control import PublisherControl
        metadata, _, etag = PublisherControl(self).read()
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
    # Exact ownership tags are required; additional tenant-policy tags are tolerated.
    tags = (resource or {}).get("tags") or {}
    if not resource or any(tags.get(key) != value
                           for key, value in {**TAGS, "sandboxId": config.sandbox}.items()):
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


def fresh_authority():
    """Initial v1 metadata for a NEW checkpoint container only; never rewritten by ARM."""
    return {"resetstate": "fenced-v1", "resetgeneration": str(uuid.uuid4()),
            "occupancy": "released", "publisherregistryversion": "1", "publishers": "{}"}


def checkpoint_container(azure):
    c = azure.c
    return azure.arm("GET", c.storage_id + "/blobServices/default/containers/" +
                     c.checkpoints, missing_ok=True, api="2023-05-01")


def deploy(azure, fresh=None):
    """Declarative ARM deployment. Never writes metadata of an existing checkpoint container.

    fresh is passed only when the container is absent; Bicep then creates it with
    fenced-v1 + empty registry + released + a fresh generation. Otherwise the
    container is not declared, so redeploy preserves all live authority state.
    Existing-sandbox callers hold a registered publisher member around this call.
    """
    c = azure.c
    guard_existing(azure)
    params = {
        "location": c.location, "resourceGroupName": c.group,
        "sandboxId": c.sandbox, "storageAccountName": c.account,
        "clusterName": c.cluster, "identityName": c.identity,
        "databaseName": c.database, "dataContainer": c.data,
        "checkpointContainer": c.checkpoints, "operatorObjectId": c.operator,
        "operatorKustoPrincipalId": c.operator_application if c.operator_type == "App" else c.operator,
        "controlMetadata": fresh or {},
        "checkpointExists": fresh is None,
        "operatorType": c.operator_type, "clusterSku": c.sku,
        "minimumInstances": c.minimum, "maximumInstances": c.maximum,
        "clusterTags": c.cluster_tags,
    }
    fd, name = tempfile.mkstemp(prefix=".env.azure.parameters.", dir=ROOT)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump({"parameters": {k: {"value": v} for k, v in params.items()}}, stream)
        os.chmod(name, 0o600)
        try:
            azure.cli("deployment", "sub", "create", "--subscription", c.subscription,
                      "--location", c.location, "--name", "arrowcol-sandbox",
                      "--template-file", str(Path(__file__).with_name("main.bicep")),
                      "--parameters", "@" + name)
        except Failure as exc:
            # `az deployment create` waits for a terminal state; a reported Failed
            # deployment is definite and performed no data-plane I/O.
            if "DeploymentFailed" in str(exc):
                raise DeploymentFailed(str(exc)) from exc
            raise
    finally:
        os.unlink(name)
    cluster = azure.arm("GET", c.cluster_id)
    c.uri = cluster["properties"]["uri"].rstrip("/")
    retry_ready(lambda: azure.blobs(c.data), "Storage RBAC")
    print("Declarative resources and storage access: ready")


def ensure_running(azure):
    """Start a Stopped cluster and wait for Kusto. Callers MUST hold a registered member.

    The member keeps stop (which requires an empty registry) from claiming its hold.
    Any start failure is an unknown outcome: the caller marks its member uncertain.
    """
    c = azure.c
    cluster = azure.arm("GET", c.cluster_id)
    owned(cluster, c)
    c.uri = cluster["properties"]["uri"].rstrip("/")
    if cluster["properties"].get("state") == "Stopped":
        azure.cli("rest", "--method", "post", "--url",
                  "https://management.azure.com" + c.cluster_id + "/start?api-version=" + API)
    retry_ready(lambda: azure.kql(".show cluster"), "Kusto")
    print("Kusto cluster: running")


def held(control, publisher, generation, work):
    """Run hosted-state work under a registered member.

    NotReady is raised only before any KQL after definite steps, so the member is
    clean-released. Any other exception or interruption is an unknown outcome:
    the member is marked uncertain (best effort) and is never auto-cleared.
    """
    try:
        return work()
    except (NotReady, DeploymentFailed):
        control.clean_release(publisher, generation, definitively_drained=True)
        raise
    except BaseException:
        try:
            control.uncertain(publisher, generation)
        except Exception:
            pass  # Still active: blocks fence, cleanup and stop.
        raise


def exclusive(control, generation, steps):
    """Run steps under exclusive operator custody claimed atomically from ready-v1.

    Each step is preceded by an ownership re-read. Success, or NotReady (raised
    only before KQL after definite steps), returns to ready-v1 through
    complete_bootstrap. Any other outcome marks the operator uncertain, leaving
    the sandbox fenced and blocking for explicit reconciliation.
    """
    operator = str(uuid.uuid4())
    control.claim_exclusive(operator, generation)
    try:
        for step in steps:
            control.bootstrap_owner(operator, generation)
            step()
    except (NotReady, DeploymentFailed):
        control.complete_bootstrap(operator, generation, definitively_drained=True)
        raise
    except BaseException:
        try:
            control.uncertain(operator, generation)
        except Exception:
            pass
        raise
    control.complete_bootstrap(operator, generation, definitively_drained=True)


def initializer_env(values, c, tables, generation):
    """Environment for the Delta initializer child; the captured generation is its only authority."""
    return {
        **os.environ, **values, **c.generated(),
        "ARROWCOL_AZURE_TABLE_MANIFEST": str(tables.resolve()),
        "ARROWCOL_EXPECTED_RESET_GENERATION": generation,
    }


def provision(azure, tables, wait_seconds, output, infra_only, values):
    from publisher_control import FENCED, READY, PublisherControl
    c = azure.c
    control = PublisherControl(azure)
    guard_existing(azure)
    if checkpoint_container(azure) is None:
        fresh = fresh_authority()
        deploy(azure, fresh)
        metadata, registry, _ = control.read()
        if (metadata["resetgeneration"] != fresh["resetgeneration"] or
                metadata["resetstate"] != FENCED or registry or
                metadata["occupancy"] != "released"):
            raise Failure("New checkpoint authority does not match its initialized generation; "
                          "concurrent first provisioning is unsupported.")
        generation = fresh["resetgeneration"]
    else:
        metadata, registry, _ = control.read()
        require_released(metadata)
        generation = metadata["resetgeneration"]
        member = str(uuid.uuid4())
        if metadata["resetstate"] == READY:
            steps = [lambda: deploy(azure), lambda: ensure_running(azure)]
            if not infra_only:
                steps.append(lambda: reconcile(azure, tables, wait_seconds))
            exclusive(control, generation, steps)
            write_private(output, {**c.generated(), "ARROWCOL_RESET_GENERATION": generation})
            print("Ready sandbox reconciled under exclusive operator custody; authority preserved.")
            return
        if metadata["resetstate"] != FENCED or registry:
            raise Failure("Authority is cleanup-owned or has registered publishers; provisioning refused.")
        control.admit(member, generation, bootstrap=True)
        held(control, member, generation, lambda: deploy(azure))
        control.clean_release(member, generation, definitively_drained=True)
    write_private(output, c.generated())
    if infra_only:
        print("INFRA-ONLY: Delta initialization and external-table/QAP reconciliation NOT verified.")
        return
    initializer = ROOT / "infra/azure/initialize-delta.sh"
    if not initializer.is_file():
        raise Failure("Delta initializer is unavailable; infra exists, full bootstrap is NOT complete.")
    # The child admits its own UUID only if its fresh session generation equals this
    # captured value, drains and unregisters it. Nothing is adopted across processes.
    result = run_tree(["bash", str(initializer)], 1800, cwd=ROOT,
                      env=initializer_env(values, c, tables, generation))
    if result.returncode:
        raise Failure("Delta initialization failed; KQL reconciliation was not attempted. " +
                      c.redact(result.stderr))
    # Fresh parent member BEFORE any Kusto/ARM I/O; fails closed if cleanup or a
    # stop hold won after the child drained.
    parent = str(uuid.uuid4())
    control.admit(parent, generation, bootstrap=True)
    def bootstrap_work():
        control.bootstrap_owner(parent, generation)
        ensure_running(azure)
        control.bootstrap_owner(parent, generation)
        reconcile(azure, tables, wait_seconds)
        control.bootstrap_owner(parent, generation)
    held(control, parent, generation, bootstrap_work)
    control.complete_bootstrap(parent, generation, definitively_drained=True)
    metadata, _, _ = control.read()
    if metadata["resetstate"] != READY or metadata["resetgeneration"] != generation:
        raise Failure("Authority changed after bootstrap completion; not exporting readiness.")
    write_private(output, {**c.generated(), "ARROWCOL_RESET_GENERATION": generation})
    print("Full bootstrap ready-v1 at the captured generation.")

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
    for name in (c.data, c.checkpoints):
        # ARM list omits container metadata, and Blob metadata names are case-preserving.
        container = azure.arm("GET", c.storage_id + "/blobServices/default/containers/" + name,
                              api="2023-05-01")
        metadata = {key.lower(): value for key, value in
                    container["properties"].get("metadata", {}).items()}
        if metadata.get("sandboxid") != c.sandbox:
            raise Failure("Container ownership metadata mismatch.")
    inventory = {name: azure.blobs(name) for name in (c.data, c.checkpoints)}
    for name in (c.data, c.checkpoints):
        by_name = {item["name"]: item for item in inventory[name]}
        for path in azure.paths(name):
            entry = by_name.get(path["name"])
            directory = str(path.get("isDirectory", False)).lower() == "true"
            if entry is not None:
                entry["directory"] = directory
                entry["etag"] = entry.get("etag") or path.get("etag")
            else:
                inventory[name].append({
                    "name": path["name"], "directory": directory, "etag": path.get("etag"),
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


def reset(azure, execute, stop_command, resume=False):
    """Data-plane teardown: preview by default; --execute deletes, --resume continues custody."""
    if resume:
        return _unqualified_reset_resume(azure)
    if execute:
        return _unqualified_reset_sequence(azure, stop_command)
    return reset_preview(azure)


def reset_preview(azure):
    databases, inventory = reset_inventory(azure)
    count = sum(len(items) for items in inventory.values())
    print(f"Reset preview: owned databases={len(databases)}, containers=2, stored objects={count}")
    print("Preserve cluster, account, containers and managed identity.")


RESET_RECORD = ROOT / ".env.azure.reset.json"
RECEIPT_FIELDS = {"stopped": True, "scope": "workspace-owned-loadtest", "dataPreserved": True}


def require_running(azure):
    """Reset never starts or stops the cluster; it refuses unless Running."""
    cluster = azure.arm("GET", azure.c.cluster_id)
    owned(cluster, azure.c)
    if cluster["properties"].get("state") != "Running":
        raise Failure("Reset requires the owned cluster to be Running; refused without "
                      "starting it (no reset-owned start/stop).")


def read_reset_record(path):
    from publisher_control import canonical_uuid
    try:
        record = json.loads(path.read_text())
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise Failure("Cleanup custody record is unreadable; refusing.") from exc
    if not isinstance(record, dict) or set(record) != {"sandbox", "operator", "generation", "next"}:
        raise Failure("Cleanup custody record is malformed; refusing.")
    for key in ("operator", "generation", "next"):
        canonical_uuid(record[key])
    return record


def record_bytes(record):
    return json.dumps(record, sort_keys=True, separators=(",", ":")).encode()


def fsync_directory(directory):
    try:
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise Failure("Cleanup custody record directory could not be made durable; "
                      "no claim or deletion attempted.") from exc


@contextlib.contextmanager
def reset_lock(path):
    """Exclusive local ownership across record read -> publish -> claim -> cleanup."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path.with_name(path.name + ".lock"), os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Failure("Another reset process holds the local custody lock; refusing.") from exc
        yield
    finally:
        os.close(fd)  # closing the descriptor releases the flock


def publish_private(path, record, *, previous=None):
    """Durably publish a custody file (file fsync, then parent fsync).

    previous=None creates-if-absent via link() and never overwrites. Otherwise the
    current bytes must equal this owner's previous record before an atomic replace.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if previous is not None and path.read_bytes() != record_bytes(previous):
        raise Failure("Custody file is not this operator's; never overwriting it.")
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".env.azure.reset.tmp.")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(record_bytes(record))
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        if previous is None:
            try:
                os.link(temporary, path)
            except FileExistsError as exc:
                raise Failure("A cleanup custody record already exists; never overwriting it.") from exc
        else:
            os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    fsync_directory(path.parent)


def write_reset_record(path, record):
    """Create-if-absent, durably, BEFORE the claim CAS; resume re-fsyncs before acting."""
    publish_private(path, record)


def remove_reset_record(path, record):
    """Remove only this operator's byte-identical record, durably."""
    try:
        current = path.read_bytes()
    except OSError as exc:
        raise Failure("Cleanup custody record is unreadable; not removing it.") from exc
    if current != record_bytes(record):
        raise Failure("Cleanup custody record is not this operator's; never removing it.")
    path.unlink()
    fsync_directory(path.parent)


DATABASE_STATES = {"submitting", "accepted", "completed"}


def database_intent_path(record_path):
    return record_path.with_name(".env.azure.reset.database.json")


PROVIDER_STATUSES = {name.lower(): name for name in ("Succeeded", "Failed", "Canceled", "InProgress")}
# Non-terminal ARM provider states observed on Microsoft.Kusto (e.g. database DELETE reports Running).
PROVIDER_STATUSES.update({name: "InProgress" for name in ("running", "accepted", "deleting")})
OPERATION_SAFE = re.compile(r"^[A-Za-z0-9/._~%=&?:+,;@!$'()*-]*$")
# ARM signs async-operation URLs (t/c/s/h query parameters); Kusto's exceed 3 KB.
MAX_OPERATION_URL = 8192


def operation_url(url):
    """Strictly validate a persisted ARM async-operation URL before any authenticated use."""
    if not isinstance(url, str) or len(url) > MAX_OPERATION_URL or not OPERATION_SAFE.match(url):
        raise Failure("Database delete operation URL is malformed; retaining custody.")
    parts = urllib.parse.urlsplit(url)
    if (parts.scheme != "https" or parts.netloc != "management.azure.com" or
            parts.hostname != "management.azure.com" or parts.username or parts.password or
            parts.port or parts.fragment or not parts.path.startswith("/") or "\\" in url or
            ".." in parts.path.split("/")):
        raise Failure("Database delete operation URL is not an ARM operation; retaining custody.")
    return url


def read_database_intent(record_path, record):
    """The owned database-DELETE intent bound to this exact custody, or None."""
    path = database_intent_path(record_path)
    try:
        raw = path.read_bytes()
        intent = json.loads(raw)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise Failure("Database delete intent is unreadable; refusing.") from exc
    if (not isinstance(intent, dict) or set(intent) != {"operator", "generation", "state", "operation"} or
            intent["state"] not in DATABASE_STATES or raw != record_bytes(intent)):
        raise Failure("Database delete intent is malformed; refusing.")
    if (intent["operator"], intent["generation"]) != (record["operator"], record["generation"]):
        raise Failure("Database delete intent belongs to other custody; never adopting it.")
    if intent["state"] == "accepted" or intent["operation"] is not None:
        operation_url(intent["operation"])
    if intent["state"] == "submitting" and intent["operation"] is not None:
        raise Failure("Database delete intent is malformed; refusing.")
    return intent


UNKNOWN_DATABASE_DELETE = (
    "Owned database DELETE submission outcome is unknown and no provider operation receipt "
    "was captured. A late request could delete a successor database after rotation, so this "
    "custody is UNRESUMABLE: cleaning-v1, the custody record and the intent are retained; "
    "absence in a listing does not reconcile it.")


def settle_database(azure, record, record_path, databases):
    """Delete the owned database only under a durable intent with a provider receipt."""
    path = database_intent_path(record_path)
    intent = read_database_intent(record_path, record)
    if intent is None:
        if not databases:
            return  # never submitted by this custody: absence is genuine
        if databases[0].get("properties", {}).get("provisioningState") == "Deleting":
            raise Failure("Owned database is Deleting without this custody's intent; refusing.")
        intent = {"operator": record["operator"], "generation": record["generation"],
                  "state": "submitting", "operation": None}
        publish_private(path, intent)  # durable BEFORE submission
        try:
            status, operation = azure.delete_database()
        except Failure as exc:
            raise Failure(UNKNOWN_DATABASE_DELETE) from exc
        if status in (200, 204, 404):
            update = {**intent, "state": "completed"}
        elif status == 202 and operation is not None:
            try:
                operation = operation_url(operation)
            except Failure as exc:
                raise Failure(UNKNOWN_DATABASE_DELETE) from exc
            update = {**intent, "state": "accepted", "operation": operation}
        else:
            raise Failure(UNKNOWN_DATABASE_DELETE)
        publish_private(path, update, previous=intent)
        intent = update
    if intent["state"] == "submitting":
        raise Failure(UNKNOWN_DATABASE_DELETE)
    if intent["state"] == "accepted":
        def terminal():
            status = azure.provider_operation(intent["operation"])
            if status == "InProgress":
                raise Failure("Database delete provider operation is still in progress.")
            return status
        operation_url(intent["operation"])  # every resume re-validates before any token
        status = retry_ready(terminal, "Database delete operation")
        if status in ("Failed", "Canceled"):
            # Explicit provider-terminal failure: that request can have no later effect.
            remove_custody_file(path, intent)
            raise Failure(f"Provider reported the database delete {status}; resume may resubmit.")
        if status != "Succeeded":
            raise Failure("Database delete provider receipt is unknown; accepted intent and "
                          "custody retained, never cleared or resubmitted.")
        update = {**intent, "state": "completed"}
        publish_private(path, update, previous=intent)


def remove_custody_file(path, record):
    if path.read_bytes() != record_bytes(record):
        raise Failure("Custody file is not this operator's; never removing it.")
    path.unlink()
    fsync_directory(path.parent)


def verified_stop_receipt(stdout, generation):
    for line in stdout.splitlines():
        try:
            receipt = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (isinstance(receipt, dict) and
                all(receipt.get(key) == value for key, value in RECEIPT_FIELDS.items()) and
                receipt.get("resetGeneration") == generation):
            return True
    return False


def _unqualified_reset_sequence(azure, stop_command, record_path=None):
    """v1 operator sequence behind public reset --execute (user-owned disposable sandbox)."""
    record_path = record_path or RESET_RECORD
    with reset_lock(record_path):
        _locked_reset_sequence(azure, stop_command, record_path)


def _locked_reset_sequence(azure, stop_command, record_path):
    from publisher_control import FENCED, READY, PublisherControl, UnknownTransition
    c = azure.c
    if read_reset_record(record_path) is not None or database_intent_path(record_path).exists():
        raise Failure("A cleanup custody record exists; resume it instead of a fresh claim.")
    control = PublisherControl(azure)
    databases, inventory = reset_inventory(azure)
    count = sum(len(items) for items in inventory.values())
    print(f"Reset preview: owned databases={len(databases)}, containers=2, stored objects={count}")
    print("Preserve cluster, account, containers and managed identity.")
    if not stop_command:
        raise Failure("Execution requires the runner's --stop-command to stop local writers/consumers.")
    require_running(azure)
    metadata, _, _ = control.read()
    require_released(metadata)
    if metadata["resetstate"] not in (READY, FENCED):
        raise Failure("Authority is not ready-v1 or fenced-v1; use reset --resume for existing "
                      "cleanup custody. Nothing was stopped or deleted.")
    generation = metadata["resetgeneration"]
    if metadata["resetstate"] == READY:
        control.fence(generation)
    result = run_tree(stop_command, 180, cwd=ROOT,
                      env={**os.environ, "ARROWCOL_RESET_GENERATION": generation})
    if result.returncode:
        raise Failure("Writer/consumer stop failed; reset fence retained, no workload data deleted. " +
                      c.redact(str(result.stderr)))
    if not verified_stop_receipt(result.stdout, generation):
        raise Failure("Runner did not verify workspace-owned services stopped for this reset "
                      "generation; reset fence retained.")
    print("Distributed reset fence active; waiting 60s for bounded in-flight publications.")
    time.sleep(60)
    # Persist operator, captured generation and the fresh successor generation
    # before the claim CAS so any later outcome is attributable without adoption.
    record = {"sandbox": c.sandbox, "operator": str(uuid.uuid4()),
              "generation": generation, "next": str(uuid.uuid4())}
    write_reset_record(record_path, record)
    try:
        control.claim_cleanup(record["operator"], generation)
    except UnknownTransition:
        raise Failure("Cleanup claim outcome unknown; custody record kept for read-only resume.")
    except Failure as exc:
        remove_reset_record(record_path, record)  # Definite refusal: no custody exists.
        raise Failure("Reset authority changed during quiescence or publishers remain; "
                      "refusing data deletion.") from exc
    running_or_abort(azure, control, record, record_path)
    cleanup_with_custody(azure, control, record, record_path)


def running_or_abort(azure, control, record, record_path):
    """After a fresh claim (no deletion yet): a stop that won during quiescence aborts.

    Stop may claim its hold from fenced-v1+empty before the cleanup claim and leave
    the cluster Stopped; cleaning-v1 then blocks every start path. The unused
    custody returns to the identical pre-claim authority instead of deleting.
    """
    try:
        require_running(azure)
    except Failure:
        control.abort_cleanup(record["operator"], record["generation"], nothing_deleted=True)
        remove_reset_record(record_path, record)
        raise Failure("Cluster is not Running after the cleanup claim (a stop won); "
                      "custody aborted before any deletion. Start via provision, then retry.")


def _unqualified_reset_resume(azure, record_path=None):
    """Recovery after an interrupted or ambiguous cleanup (public reset --resume).

    Re-reads authority and continues only with this record's exact operator and
    generation. Never takes over or clears a foreign or uncertain member, and
    never starts or stops the cluster.
    """
    record_path = record_path or RESET_RECORD
    with reset_lock(record_path):
        _locked_reset_resume(azure, record_path)


def _locked_reset_resume(azure, record_path):
    from publisher_control import CLEANING, FENCED, PublisherControl, UnknownTransition
    record = read_reset_record(record_path)
    if record is None:
        raise Failure("No cleanup custody record; nothing to resume.")
    if record_path.read_bytes() != record_bytes(record):
        raise Failure("Cleanup custody record is not in canonical form; refusing.")
    if record["sandbox"] != azure.c.sandbox:
        raise Failure("Cleanup custody record belongs to another sandbox; refusing.")
    # Durability repair: a crash may have preceded the parent fsync.
    fsync_directory(record_path.parent)
    control = PublisherControl(azure)
    metadata, registry, _ = control.read()
    operator, generation = record["operator"], record["generation"]
    if metadata["resetgeneration"] == record["next"]:
        # Only this record knows the fresh successor: the finish CAS applied earlier,
        # which requires a completed database intent first.
        intent = read_database_intent(record_path, record)
        if intent is not None:
            if intent["state"] != "completed":
                raise Failure("Rotated authority with an incomplete database intent; refusing.")
            remove_custody_file(database_intent_path(record_path), intent)
        remove_reset_record(record_path, record)
        print("Earlier cleanup already finished and rotated authority; record cleared.")
        return
    if metadata["resetgeneration"] != generation:
        raise Failure("Authority generation is neither this record's nor its successor; refusing.")
    if metadata["resetstate"] == FENCED and not registry:
        # The claim never applied (leaving cleaning-v1 always rotates generation).
        require_running(azure)
        try:
            control.claim_cleanup(operator, generation)
        except UnknownTransition:
            raise Failure("Cleanup claim outcome unknown again; custody record kept.")
        running_or_abort(azure, control, record, record_path)
    elif not (metadata["resetstate"] == CLEANING and
              registry == {operator: {"generation": generation, "state": "active"}}):
        raise Failure("Authority does not show this record's exact active cleanup custody; "
                      "never taking over or clearing another or uncertain member.")
    cleanup_with_custody(azure, control, record, record_path)



def cleanup_with_custody(azure, control, record, record_path):
    """Idempotent deletion under exclusive cleaning-v1 custody, then rotate.

    Every step re-reads current state, so applied-but-unacknowledged deletes are
    simply absent. Any failure keeps cleaning-v1 and the record for resume.
    """
    c = azure.c
    operator, generation = record["operator"], record["generation"]
    control.cleanup_operator(operator, generation)
    require_running(azure)
    databases, inventory = reset_inventory(azure)
    if any(not (blob["version"] or blob["snapshot"] or blob.get("etag"))
           for blobs in inventory.values() for blob in blobs):
        raise Failure("Inventoried object has no ETag; refusing an unconditional delete.")
    control.cleanup_operator(operator, generation)
    settle_database(azure, record, record_path, databases)
    for container, blobs in inventory.items():
        control.cleanup_operator(operator, generation)
        for blob in sorted(blobs, key=lambda item: item["name"].count("/"), reverse=True):
            query = {}
            if blob["version"]:
                query["versionid"] = blob["version"]
            elif blob["snapshot"]:
                query["snapshot"] = blob["snapshot"]
            if query:  # immutable snapshot/version identity cannot name a successor object
                azure.blob("DELETE", container, blob["name"], query=query, missing_ok=True)
                continue
            # Base blobs and HNS paths: delete only the exact inventoried object, so an
            # unknown in-flight request landing late cannot remove a successor at the path.
            if blob.get("directory"):
                azure.delete_directory(container, blob["name"], blob["etag"])
            else:
                azure.blob("DELETE", container, blob["name"],
                           headers={"x-ms-delete-snapshots": "include", "If-Match": blob["etag"]},
                           missing_ok=True)
    remaining = [b for name in (c.data, c.checkpoints) for b in azure.blobs(name)]
    remaining.extend(path for name in (c.data, c.checkpoints) for path in azure.paths(name))
    if remaining:
        raise Failure("Reset left workload state behind; cleanup authority retained.")
    settle_database(azure, record, record_path, [])  # re-verify the receipt, never infer it
    intent = read_database_intent(record_path, record)
    if intent is not None and intent["state"] != "completed":
        raise Failure(UNKNOWN_DATABASE_DELETE)
    retry_ready(lambda: no_databases(azure), "Database deletion")
    control.finish_cleanup(operator, generation, record["next"], definitively_cleaned=True)
    if intent is not None:
        remove_custody_file(database_intent_path(record_path), intent)
    remove_reset_record(record_path, record)
    print("Reset verified: zero databases and zero blobs, versions or snapshots; "
          "authority is fenced-v1 at a fresh generation.")


def no_databases(azure):
    if azure.collection(azure.c.cluster_id + "/databases"):
        raise Failure("Database deletion is still in progress.")

def require_released(metadata):
    # v1 occupancy is mandatory; absence is never treated as released.
    if metadata.get("occupancy") != "released":
        raise Failure("Cloud tests or a stop hold own the sandbox; wait for explicit release.")

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
    # Same as Rust Registry::parse: only canonical compact sorted JSON is valid wire.
    if json.dumps(registry, separators=(",", ":"), sort_keys=True) != raw:
        raise Failure("Publisher registry is not canonical compact JSON.")
    return registry


def require_drained_publishers(metadata):
    if publisher_registry(metadata):
        raise Failure("Registered active or uncertain publishers block destructive cleanup; no expiry or force-clear is permitted.")


def require_released_or_owner(metadata, owner):
    if metadata.get("occupancy") == "released":
        return
    if (metadata.get("occupancy") != "testing" or not owner or
            metadata.get("occupancyid") != owner):
        raise Failure("Only the active cloud test owner may reconcile isolated table bindings.")


def reconcile_command(azure, tables, owner, wait_seconds=0, expected=None):
    """Public reconcile: registered normal member around cluster start and all KQL.

    Under test occupancy the caller's captured generation is mandatory; the
    member is admitted only at that generation (admit re-checks it in the
    leased transition), so a rotated sandbox fails before any ARM/KQL I/O.
    """
    from publisher_control import PublisherControl
    control = PublisherControl(azure)
    metadata, _, _ = control.read()
    require_released_or_owner(metadata, owner)
    if metadata["occupancy"] == "testing" and not expected:
        raise Failure("Reconcile under test occupancy requires the captured "
                      "ARROWCOL_RESET_GENERATION (or --reset-generation).")
    generation = expected or metadata["resetgeneration"]
    if generation != metadata["resetgeneration"]:
        raise Failure("Reset generation changed since occupancy; reconcile refused before I/O.")
    if metadata["occupancy"] == "released":
        # Released: exclusive custody, so a later occupy cannot start under this KQL.
        exclusive(control, generation, [lambda: ensure_running(azure),
                                        lambda: reconcile(azure, tables, wait_seconds)])
        return
    # Testing: the owner/generation predicate is re-checked inside the admit CAS.
    member = str(uuid.uuid4())
    control.admit(member, generation, occupant=owner)
    def work():
        ensure_running(azure)
        reconcile(azure, tables, wait_seconds)
    held(control, member, generation, work)
    control.clean_release(member, generation, definitively_drained=True)


def occupancy(azure, action, owner, output=None, generation=None):
    from publisher_control import READY, PublisherControl
    try:
        owner = str(uuid.UUID(owner))
    except ValueError as exc:
        raise Failure("Occupancy requires the runner's UUID --occupancy-id.") from exc
    metadata, registry, etag = PublisherControl(azure).read()
    current = metadata["occupancy"]
    if action == "occupy":
        if metadata["resetstate"] != READY:
            raise Failure("Cannot start cloud tests unless authority is ready-v1.")
        if current == "stopping":
            raise Failure("A cluster stop holds the sandbox; tests cannot start.")
        if current != "released" and (current != "testing" or metadata.get("occupancyid") != owner):
            raise Failure("Another cloud test owns the sandbox.")
        metadata.update(occupancy="testing", occupancyid=owner)
    else:
        if current != "testing" or metadata.get("occupancyid") != owner:
            raise Failure("Only the exact occupancy owner can release cloud tests.")
        if not generation or metadata["resetgeneration"] != generation:
            raise Failure("Release requires the captured ARROWCOL_RESET_GENERATION of this test.")
        if registry:
            # Runner drain receipts are not registry close; never force-clear members.
            raise Failure("Registered active or uncertain publishers remain; occupancy retained.")
        metadata.update(occupancy="released")
    # The leased CAS compares the full read (state, generation, registry, owner).
    azure.set_control(metadata, etag)
    if action == "occupy" and output is not None:
        # Re-read after CAS: never infer generation from the occupancy/run UUID.
        current, _ = azure.control()
        if (current.get("occupancyid") != owner or current.get("occupancy") != "testing" or
                current.get("resetstate") != READY or
                current.get("resetgeneration") != metadata["resetgeneration"]):
            raise Failure("Authority changed after occupancy; do not admit collectors.")
        values = azure.c.generated()
        values["ARROWCOL_RESET_GENERATION"] = current["resetgeneration"]
        write_private(output, values)
    print("Cloud test occupancy: " + ("TESTING" if action == "occupy" else "RELEASED"))

def recover(azure, execute, stop_command):
    """Unstick a sandbox stranded by a crashed run (orphaned publishers/occupancy)."""
    from publisher_control import PublisherControl, UnknownTransition
    control = PublisherControl(azure)
    metadata, registry, _ = control.read()
    print(f"Recover preview: state={metadata['resetstate']}, occupancy={metadata['occupancy']}, "
          f"registered publishers={len(registry)}")
    if not execute:
        print("Recover --execute stops local writers, fences, rotates the generation, clears the "
              "registry and releases occupancy. Then run reset --execute and provision.")
        return
    generation = metadata["resetgeneration"]
    if metadata["resetstate"] not in ("ready-v1", "fenced-v1") or metadata["occupancy"] == "stopping":
        raise Failure("Cleanup or a cluster stop owns authority; recovery refused before any stop.")
    result = run_tree(stop_command, 180, cwd=ROOT,
                      env={**os.environ, "ARROWCOL_RESET_GENERATION": generation})
    if result.returncode or not verified_stop_receipt(result.stdout, generation):
        raise Failure("Runner did not verify workspace-owned services stopped; nothing recovered. " +
                      azure.c.redact(str(result.stderr)))
    print("Local writers stopped; waiting 60s for bounded in-flight publications.")
    time.sleep(60)
    try:
        control.abandon(generation, str(uuid.uuid4()), writers_stopped=True)
    except UnknownTransition:
        raise Failure("Recovery CAS outcome unknown; re-read authority with recover (preview).")
    print("Recovered: fenced-v1, released, empty registry, fresh generation. "
          "Next: reset --execute, then provision.")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("config", "preflight", "provision", "reconcile", "reset", "recover", "status", "stop", "occupy", "release"))
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--output", type=Path, default=ROOT / ".env.azure")
    parser.add_argument("--tables", type=Path, default=ROOT / ".env.azure.tables.json")
    parser.add_argument("--infra-only", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--occupancy-id", default="")
    parser.add_argument("--reset-generation", default="")
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
            provision(azure, args.tables, args.wait_seconds, args.output, args.infra_only, values)
        elif args.command == "reconcile":
            reconcile_command(azure, args.tables, args.occupancy_id, args.wait_seconds,
                              args.reset_generation or values.get("ARROWCOL_RESET_GENERATION"))
        elif args.command == "reset":
            reset(azure, args.execute, args.stop_command or DEFAULT_STOP_COMMAND, resume=args.resume)
        elif args.command == "recover":
            recover(azure, args.execute, args.stop_command or DEFAULT_STOP_COMMAND)
        elif args.command == "status":
            databases, _ = reset_inventory(azure)
            if not databases:
                print("Owned infrastructure: present; Kusto database absent (data plane reset). "
                      "Run provision to recreate it.")
                return 0
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
                      args.output if args.command == "occupy" else None,
                      args.reset_generation or values.get("ARROWCOL_RESET_GENERATION"))
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
    """Serialize stop with occupy, bootstrap and start through a persistent stopping hold.

    The hold is claimed from released + EMPTY registry + ready-v1/fenced-v1 in one
    leased CAS before any ARM call. A registered normal or bootstrap member (held
    around every public start/KQL) therefore blocks stop, and every admission
    rejects occupancy=stopping. The hold is released only after this process's
    own definite ARM submission and an observed Stopped state. An existing hold
    is never replayed or released: a prior submission may still complete late.
    Any unknown CAS, submission or wait outcome leaves a blocking hold for
    explicit operator reconciliation. A definite CAS conflict stays retryable.
    """
    from publisher_control import FENCED, READY, PublisherControl
    metadata, registry, etag = PublisherControl(azure).read()
    current = metadata["occupancy"]
    if current == "stopping":
        raise Failure("A prior stop hold may have an ambiguous ARM submission; not replaying "
                      "or releasing it. Reconcile the old request explicitly.")
    if current != "released":
        raise Failure("Cloud tests own the sandbox; wait for explicit occupancy release.")
    if metadata["resetstate"] not in (READY, FENCED):
        raise Failure("Cleanup owns the authority; stop refused.")
    if registry:
        raise Failure("Registered publishers or bootstrap owners protect the cluster; stop refused.")
    hold = str(uuid.uuid4())
    metadata.update(occupancy="stopping", occupancyid=hold)
    azure.set_control(metadata, etag)
    azure.cli("rest", "--method", "post", "--url", "https://management.azure.com" +
              azure.c.cluster_id + "/stop?api-version=" + API)
    retry_ready(lambda: stopped(azure), "Cluster stop", timeout=1200)
    metadata, etag = azure.control()
    if metadata.get("occupancy") != "stopping" or metadata.get("occupancyid") != hold:
        raise Failure("Stop hold changed unexpectedly; reconcile occupancy before reuse.")
    metadata["occupancy"] = "released"
    azure.set_control(metadata, etag)

def stopped(azure):
    if azure.arm("GET", azure.c.cluster_id)["properties"].get("state") != "Stopped":
        raise Failure("Cluster is still stopping.")


if __name__ == "__main__":
    sys.exit(main())
