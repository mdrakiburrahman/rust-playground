"""Serialized policy-effect core; not enabled by the read-only HTTP transport."""
import hashlib
import http.client
import threading
import urllib.error

from live_contract import Denial, SNAPSHOT_AGE_NS, authorize_command, validate_packet
from live_ledger import TERMINAL


UNCERTAIN = (OSError, ValueError, RuntimeError, http.client.HTTPException, urllib.error.URLError)


class PolicyExecutor:
    """Adapters are trusted server-owned resource, authority and snapshot probes."""

    def __init__(self, ledger, post, resource, snapshot):
        self.ledger = ledger
        self.post = post
        self.resource = resource
        self.snapshot = snapshot
        self.lock = threading.Lock()

    @staticmethod
    def native_operation(operation_id, deployment):
        value = deployment["id"] + "\0" + deployment["incarnation"] + "\0" + operation_id
        return "bridge-" + hashlib.sha256(value.encode()).hexdigest()

    def desired(self, intent, state):
        action = intent["action"]
        target = action["target"]
        writer = target["writer"]
        if self.resource(writer["collector"]) != target["resource"]:
            raise Denial("wrong-target")
        if not any(member["writer"] == writer for member in state["members"]):
            raise Denial("wrong-target")
        old = state["policies"].get(writer["collector"])
        revision = int(target["policy_revision"])
        if revision >= 2**63 - 1:
            raise Denial("limit")
        if old is not None and old.get("incarnation") != writer:
            raise Denial("wrong-target")
        if (old or {}).get("revision", 0) != revision:
            raise Denial("stale")
        if old and (old["retiring"] or old["approved"]):
            raise Denial("conflict")
        flags = {"excluded": (old or {}).get("excluded", False),
                 "block_streams": (old or {}).get("block_streams", False), "retiring": False}
        flags["excluded" if action["operation"] == "exclusion" else "block_streams"] = action["enabled"]
        return flags

    def apply(self, packet, deployment, session, origin):
        authorize_command(packet, deployment, session, origin)
        if packet["action"]["operation"] not in ("exclusion", "admission"):
            raise Denial("unsupported")
        if not self.lock.acquire(blocking=False):
            raise Denial("limit")
        try:
            result, fresh = self.ledger.reserve(packet)
            if not fresh:
                return result
            operation_id, identity = packet["operation_id"], packet["deployment"]
            intent = self.ledger.intent(operation_id, identity)
            try:
                flags = self.desired(intent, self.post({"op": "view"}))
                authorize_command(packet, deployment, session, origin)
            except Denial as error:
                return self.ledger.advance(operation_id, identity, "rejected", error_code=error.code)
            except UNCERTAIN:
                return self.ledger.advance(operation_id, identity, "failed", error_code="disconnected")
            target = packet["action"]["target"]
            self.ledger.advance(operation_id, identity, "pending")
            try:
                self.post({
                    "op": "policy", "collector": target["writer"]["collector"], "writer": target["writer"],
                    "revision": int(target["policy_revision"]),
                    "operation": self.native_operation(operation_id, identity), **flags,
                })
            except UNCERTAIN:
                return self.ledger.advance(operation_id, identity, "unknown", error_code="disconnected")
            return self._reconcile(operation_id, identity, flags)
        finally:
            self.lock.release()

    def reconcile(self, operation_id, deployment):
        """Observe stored intent, never replay a policy mutation after restart/loss."""
        with self.lock:
            return self._reconcile(operation_id, deployment)

    def _reconcile(self, operation_id, deployment, expected=None):
        result = self.ledger.get(operation_id, deployment)
        if result["status"] in TERMINAL:
            return result
        intent = self.ledger.intent(operation_id, deployment)
        action = intent["action"]
        if action["operation"] not in ("exclusion", "admission"):
            raise Denial("unsupported")
        target = action["target"]
        desired_revision = int(target["policy_revision"]) + 1
        try:
            if self.resource(target["writer"]["collector"]) != target["resource"]:
                raise Denial("wrong-target")
            state = self.post({"op": "view"})
            policy = state["policies"].get(target["writer"]["collector"])
            if (policy is None or policy.get("incarnation") != target["writer"]
                    or policy["operation"] != self.native_operation(operation_id, deployment)
                    or policy["revision"] != desired_revision or policy["retiring"]):
                raise Denial("stale")
            field = "excluded" if action["operation"] == "exclusion" else "block_streams"
            if policy[field] != action["enabled"] or (
                expected is not None and any(policy[key] != value for key, value in expected.items())
            ):
                raise Denial("conflict")
            snapshot = validate_packet(self.snapshot())
            freshness = snapshot["freshness"]
            if (snapshot["deployment"] != deployment or freshness["status"] != "fresh"
                    or freshness["age_ns"] is None or int(freshness["age_ns"]) > SNAPSHOT_AGE_NS):
                raise Denial("stale")
            replica = next((replica for replica in snapshot["replicas"]
                            if replica["resource"] == target["resource"] and replica["writer"] == target["writer"]), None)
            if (replica is None or replica["policy_revision"] != str(desired_revision)
                    or any(replica[key] != policy[key] for key in ("excluded", "block_streams", "retiring"))):
                raise Denial("stale")
            if result["status"] != "applied":
                result = self.ledger.advance(operation_id, deployment, "applied", snapshot["revision"])
            if (policy["applied_revision"] == desired_revision
                    and replica["applied_policy_revision"] == str(desired_revision)):
                return self.ledger.advance(operation_id, deployment, "completed", snapshot["revision"])
            return result
        except Denial as error:
            return self.ledger.advance(operation_id, deployment, "unknown", error_code=error.code)
        except UNCERTAIN:
            return self.ledger.advance(operation_id, deployment, "unknown", error_code="disconnected")
