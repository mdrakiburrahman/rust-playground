"""Bounded policy effects and read-only reconciliation; no automatic replay."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import threading
import urllib.error

from jsonschema.exceptions import ValidationError

from live_contract import Denial, SNAPSHOT_AGE_NS, authorize_command, validate_packet
from live_ledger import TERMINAL


UNCERTAIN = (OSError, ValueError, RuntimeError, KeyError, TypeError, ValidationError,
             http.client.HTTPException, urllib.error.URLError)


def operation_error(code):
    return code if code in {
        "disarmed", "stale", "wrong-target", "unsupported", "unqualified",
        "disconnected", "conflict", "timeout", "failed", "unknown",
    } else "failed"


class PolicyExecutor:
    """Adapters are trusted server-owned resource, authority and snapshot probes."""

    operations = frozenset({"exclusion", "admission"})

    def __init__(self, ledger, post, resource, snapshot):
        self.ledger = ledger
        self.post = post
        self.resource = resource
        self.snapshot = snapshot
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="local-policy")
        self.failed = threading.Event()

    def close(self):
        self.failed.set()
        self.pool.shutdown(wait=True)

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

    def validate_action(self, action):
        if action["operation"] not in self.operations:
            raise Denial("unsupported")
        if int(action["target"]["policy_revision"]) >= 2**63 - 1:
            raise Denial("limit")

    def _reserve(self, packet, deployment, session, origin):
        authorize_command(packet, deployment, session, origin)
        if self.failed.is_set():
            raise Denial("disconnected")
        self.validate_action(packet["action"])
        if not self.lock.acquire(blocking=False):
            try:
                intent = self.ledger.intent(packet["operation_id"], packet["deployment"])
            except Denial as error:
                if error.code != "unknown-operation":
                    raise
                raise Denial("limit") from None
            if intent != {name: packet[name] for name in ("deployment", "revision", "action")}:
                raise Denial("conflict")
            return self.ledger.get(packet["operation_id"], packet["deployment"])
        try:
            result, fresh = self.ledger.reserve(packet)
            if not fresh:
                self.lock.release()
                return result
            return None
        except BaseException:
            self.lock.release()
            raise

    def apply(self, packet, deployment, session, origin):
        existing = self._reserve(packet, deployment, session, origin)
        if existing is not None:
            return existing
        return self._apply_reserved(packet, deployment, session, origin)

    def submit(self, packet, deployment, session, origin, recheck=None):
        existing = self._reserve(packet, deployment, session, origin)
        if existing is not None:
            return existing
        self._schedule(self._apply_reserved, packet["operation_id"], packet["deployment"],
                       packet, deployment, session, origin, recheck)
        return self.ledger.get(packet["operation_id"], packet["deployment"])

    def refresh(self, operation_id, deployment):
        result = self.ledger.get(operation_id, deployment)
        if result["status"] in TERMINAL or self.failed.is_set() or not self.lock.acquire(blocking=False):
            return result
        self._schedule(self._refresh_reserved, operation_id, deployment, operation_id, deployment)
        return result

    def _schedule(self, work, operation_id, deployment, *args):
        try:
            future = self.pool.submit(work, *args)
        except RuntimeError:
            self.lock.release()
            self.failed.set()
            self.ledger.advance(operation_id, deployment, "unknown", error_code="disconnected")
            raise Denial("disconnected")
        def finished(done):
            error = done.exception()
            if error is not None:
                self.failed.set()
                print("live bridge effect unavailable: " + type(error).__name__, flush=True)
                result = self.ledger.get(operation_id, deployment)
                if result["status"] not in TERMINAL:
                    self.ledger.advance(operation_id, deployment, "unknown", error_code="failed")
        future.add_done_callback(finished)

    def _apply_reserved(self, packet, deployment, session, origin, recheck=None):
        try:
            operation_id, identity = packet["operation_id"], packet["deployment"]
            intent = self.ledger.intent(operation_id, identity)
            try:
                if self.failed.is_set():
                    raise Denial("disconnected")
                flags = self.desired(intent, self.post({"op": "view"}))
                if self.failed.is_set():
                    raise Denial("disconnected")
                if recheck is not None:
                    recheck()
                else:
                    authorize_command(packet, deployment, session, origin)
            except Denial as error:
                return self.ledger.advance(operation_id, identity, "rejected", error_code=operation_error(error.code))
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

    def _refresh_reserved(self, operation_id, deployment):
        try:
            return self._reconcile(operation_id, deployment)
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
            return self.ledger.advance(operation_id, deployment, "unknown", error_code=operation_error(error.code))
        except UNCERTAIN:
            return self.ledger.advance(operation_id, deployment, "unknown", error_code="disconnected")
