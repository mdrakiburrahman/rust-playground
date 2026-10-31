"""Live packet validation and fail-closed policy; no transport or execution."""
from dataclasses import dataclass, field
import hmac
import json
from pathlib import Path
import time
from typing import Mapping
from urllib.parse import urlsplit

from jsonschema import Draft202012Validator

from exchange import unique_object


REQUEST_BYTES = 32768
RESPONSE_BYTES = 1048576
SNAPSHOT_AGE_NS = 5_000_000_000
ARM_TTL_NS = 30_000_000_000
DIAGNOSTIC_MISSING = frozenset({
    "cpu", "memory", "cpu-counter-gap", "io-read", "io-write",
    "queue-depth", "queue-age", "throughput",
})
SCHEMA = json.loads((Path(__file__).parent / "contracts/live-v1.schema.json").read_text())
Draft202012Validator.check_schema(SCHEMA)
VALIDATOR = Draft202012Validator(SCHEMA)


class Denial(ValueError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code

    def packet(self):
        return {"kind": "error", "schema_version": 1, "code": self.code,
                "resnapshot": self.code in ("stale", "wrong-target")}


def validate_packet(value):
    raw = json.dumps(value, allow_nan=False).encode()
    if len(raw) > RESPONSE_BYTES:
        raise Denial("limit")

    def bounded(item, depth=0):
        if depth > 16:
            raise Denial("limit")
        if isinstance(item, dict):
            for child in item.values():
                bounded(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                bounded(child, depth + 1)

    bounded(value)
    VALIDATOR.validate(value)
    if value["kind"] == "snapshot":
        replicas = value["replicas"]
        tables = value["tables"]
        receipts = value["receipts"]
        for ids in ([r["resource"]["id"] for r in replicas],
                    [t["id"] for t in tables], [r["id"] for r in receipts],
                    [m["id"] for m in value["metrics"]]):
            if len(ids) != len(set(ids)):
                raise Denial("invalid")
        table_ids = {t["id"] for t in tables}
        if any(r["table_id"] not in table_ids for r in receipts):
            raise Denial("invalid")
        resources = {r["resource"]["id"]: r["resource"] for r in replicas}
        if any(resources.get(m["resource"]["id"]) != m["resource"] for m in value["metrics"]):
            raise Denial("invalid")
        coverage = value["coverage"]
        if coverage["complete"] and (coverage["missing"] or coverage["dropped"]):
            raise Denial("invalid")
        freshness = value["freshness"]
        if freshness["status"] == "fresh" and (
            freshness["age_ns"] is None or int(freshness["age_ns"]) > SNAPSHOT_AGE_NS
        ):
            raise Denial("invalid")
    elif value["kind"] == "feed":
        if value["gap"] and (not value["resnapshot"] or value["items"]):
            raise Denial("invalid")
        previous = int(value["after"])
        for snapshot in value["items"]:
            validate_packet(snapshot)
            revision = int(snapshot["revision"])
            if (snapshot["bridge"] != value["bridge"] or snapshot["deployment"] != value["deployment"]
                    or revision != previous + 1 or revision > int(value["next"])):
                raise Denial("invalid")
            previous = revision
        if not value["gap"] and int(value["next"]) != previous:
            raise Denial("invalid")
    return value


def decode_request(raw):
    if len(raw) > REQUEST_BYTES:
        raise Denial("limit")
    packet = json.loads(raw, object_pairs_hook=unique_object,
                        parse_constant=lambda _: (_ for _ in ()).throw(Denial("invalid")))
    validate_packet(packet)
    if packet["kind"] not in ("select", "arm", "disarm", "command"):
        raise Denial("invalid")
    return packet


def require_origin(origin):
    parsed = urlsplit(origin)
    if (parsed.scheme not in ("http", "https") or parsed.hostname not in ("127.0.0.1", "::1", "localhost")
            or parsed.username is not None or parsed.password is not None
            or parsed.path or parsed.query or parsed.fragment or parsed.port is None):
        raise Denial("origin")
    return origin


def require_observations(snapshot, action=None):
    """Only current producer facts, never declarations from a command."""
    validate_packet(snapshot)
    coverage = snapshot["coverage"]
    missing = set(coverage["missing"])
    if missing - DIAGNOSTIC_MISSING or coverage["dropped"] and "cpu-counter-gap" not in missing:
        raise Denial("disconnected")
    if action is None:
        return
    operation = action["operation"]
    replicas = snapshot["replicas"]
    if operation == "workload":
        selected = [row for row in replicas if row["role"] == "gateway"]
        if snapshot["receipt_observation_known"] is not True:
            raise Denial("disconnected")
    elif operation == "replicas":
        selected = [row for row in replicas if row["role"] == action["role"]]
    else:
        resource = (action["target"]["resource"] if operation in ("admission", "exclusion", "retirement")
                    else action.get("resource"))
        selected = [row for row in replicas if row["resource"] == resource]
        if operation in ("admission", "exclusion", "retirement") and any(
                row["role"] != "collector" for row in selected):
            raise Denial("wrong-target")
    if not selected or any(row["identity_verified"] is not True or row["running"] is not True
                           or row["paused"] is not False for row in selected):
        raise Denial("wrong-target")
    policy_rows = ([row for row in selected if row["role"] == "collector"]
                   if operation == "replicas" else selected)
    if operation in ("admission", "exclusion", "retirement", "replicas") and any(
        row["writer"] is None or row["policy_revision"] is None
        or any(row[field] is None for field in ("excluded", "block_streams", "retiring"))
        or row["retiring"] is not False for row in policy_rows
    ):
        raise Denial("wrong-target")


@dataclass(frozen=True)
class DeploymentFence:
    identity: dict
    revision: str
    observed_ns: int | None = None
    connected: bool = False
    qualified: bool = False
    supported: frozenset[str] = frozenset()
    targets: Mapping[str, dict] = field(default_factory=dict)
    table_epochs: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SessionFence:
    allowed_origin: str
    authenticated: bool = False
    csrf_verified: bool = False
    mode: str = "recorded"
    deployment: dict | None = None
    selection_id: str | None = None
    arm_id: str | None = None
    arm_deadline_ns: int = 0


def authorize_command(packet, deployment, session, origin, now_ns=None):
    """Inputs are server-owned fences, never imported packet declarations."""
    validate_packet(packet)
    if packet["kind"] != "command":
        raise Denial("invalid")
    require_origin(session.allowed_origin)
    if not isinstance(origin, str) or not hmac.compare_digest(origin, session.allowed_origin):
        raise Denial("origin")
    if not session.authenticated:
        raise Denial("unauthenticated")
    if not session.csrf_verified:
        raise Denial("csrf")
    if session.mode != "live" or not deployment.connected:
        raise Denial("disconnected")
    if not deployment.qualified:
        raise Denial("unqualified")
    now = time.monotonic_ns() if now_ns is None else now_ns
    if (packet["deployment"] != deployment.identity or session.deployment != deployment.identity):
        raise Denial("wrong-target")
    if (deployment.observed_ns is None or not 0 <= now - deployment.observed_ns <= SNAPSHOT_AGE_NS
            or packet["revision"] != deployment.revision):
        raise Denial("stale")
    if (packet["selection_id"] != session.selection_id or packet["arm_id"] != session.arm_id
            or not 0 < session.arm_deadline_ns - now <= ARM_TTL_NS):
        raise Denial("disarmed")
    action = packet["action"]
    operation = action["operation"]
    if operation not in deployment.supported:
        raise Denial("unsupported")
    if operation in ("retirement", "exclusion", "admission"):
        target = action["target"]
        if deployment.targets.get(target["resource"]["id"]) != target:
            raise Denial("wrong-target")
    elif operation in ("fault", "repair"):
        target = deployment.targets.get(action["resource"]["id"])
        if target is None or target["resource"] != action["resource"]:
            raise Denial("wrong-target")
    elif operation == "migration":
        if deployment.table_epochs.get(action["table_id"]) != action["expected_epoch"]:
            raise Denial("wrong-target")
    return packet
