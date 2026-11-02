"""Private safety footprint and bounded local guard evidence; never a writer grant."""
from collections import deque
import copy
import hashlib
import json
import secrets
import threading
import time

from live_contract import DIAGNOSTIC_MISSING, RESPONSE_BYTES


def digest(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > RESPONSE_BYTES:
        raise ValueError("safety footprint exceeds bound")
    return hashlib.sha256(raw).hexdigest()


def native_safety(state):
    """Omit only known diagnostic counters; retain unclassified fields conservatively."""
    result = copy.deepcopy(state)
    result.pop("revision", None)
    for member in result["members"]:
        for field in ("pressure", "rss_bytes", "active"):
            member.pop(field, None)
    result["members"].sort(key=lambda row: digest(row["writer"]))
    result["assignments"].sort(key=lambda row: row["record"]["key"])
    for assignment in result["assignments"]:
        assignment.pop("revision", None)
    for observation in result.get("observations", {}).values():
        for field in ("sequence", "demand", "capacity", "tables"):
            observation.pop(field, None)
        cursor = observation.pop("allocation_revision", None)
        observation["allocation_revision"] = {"known": type(cursor) is int and cursor >= 0}
    for field in ("sampled_at", "sustained", "constraint"):
        result.get("optimization", {}).pop(field, None)
    return result


def footprint(snapshot, capabilities, context):
    critical_missing = sorted(set(snapshot["coverage"]["missing"]) - DIAGNOSTIC_MISSING)
    dropped_unknown = snapshot["coverage"]["dropped"] > 0 and (
        "cpu-counter-gap" not in snapshot["coverage"]["missing"])
    return digest({
        "bridge": snapshot["bridge"], "deployment": snapshot["deployment"],
        "replicas": sorted(snapshot["replicas"], key=lambda row: row["resource"]["id"]),
        "tables": sorted(snapshot["tables"], key=lambda row: row["id"]),
        "receipt_health": snapshot["receipt_observation_known"],
        "critical_missing": critical_missing, "dropped_unknown": dropped_unknown,
        "connected": snapshot["freshness"]["status"] == "fresh",
        "capabilities": capabilities,
        # A creator's own unbound reservation is fenced by its exact inventory CAS.
        "context": None if context is None else {key: value for key, value in context.items()
                                                if key != "inventory_revision"},
    })


class GuardEvidence:
    """Bounded private in-memory evidence; no public export or request secrets."""
    def __init__(self):
        self.lock = threading.Lock()
        self.records = deque()
        self.operations = set()
        self.bytes = 0
        self.dropped = 0
        self.clock_origin = time.monotonic_ns()
        self.clock_id = "bridge-monotonic-" + secrets.token_hex(16)

    def record(self, operation_id, phase, started, outcome, **facts):
        entry = {
            "operation_id": operation_id, "phase": phase, "outcome": outcome,
            "offset_ns": str(started - self.clock_origin),
            "duration_ns": str(time.monotonic_ns() - started), **facts,
        }
        size = len(json.dumps(entry, allow_nan=False).encode()) + 2
        with self.lock:
            if ((operation_id not in self.operations and len(self.operations) >= 64)
                    or self.bytes + size + 512 > 65536):
                self.dropped += 1
                return
            self.operations.add(operation_id)
            self.records.append(entry)
            self.bytes += size

    def snapshot(self):
        with self.lock:
            return {"clock": self.clock_id, "records": copy.deepcopy(list(self.records)),
                    "dropped": self.dropped, "bytes": self.bytes}

    def measure(self, operation_id, phase, function, *args, **kwargs):
        started = time.monotonic_ns()
        outcome = "ok"
        try:
            return function(*args, **kwargs)
        except Exception as error:
            outcome = type(error).__name__
            raise
        finally:
            self.record(operation_id, phase, started, outcome)
