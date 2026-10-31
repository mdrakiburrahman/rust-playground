"""Serialized guarded admission with independently bounded fault compensation."""
import subprocess
import time

from live_contract import Denial, SNAPSHOT_AGE_NS, authorize_command, validate_packet
from live_executor import PolicyExecutor, UNCERTAIN, operation_error
from live_faults import ALLOWED, MAX_DURATION_MS, boot_identity, exact_state
from live_ledger import TERMINAL
from live_workload import PRESETS, preset_for


EFFECT_UNCERTAIN = (*UNCERTAIN, subprocess.TimeoutExpired)


class EffectExecutor(PolicyExecutor):
    def __init__(self, ledger, post, resource, snapshot, faults=None, workload=None):
        super().__init__(ledger, post, resource, snapshot)
        self.faults, self.workload = faults, workload
        self.operations = frozenset({"exclusion", "admission"}
                                    | ({"fault"} if faults is not None else set())
                                    | ({"workload"} if workload is not None else set()))
        self.maximums = {"exclusion": 1, "admission": 1, "fault": MAX_DURATION_MS, "workload": 8}
        self.presets = [dict(preset) for preset in PRESETS] if workload is not None else []

    def validate_action(self, action):
        operation = action["operation"]
        if operation not in self.operations:
            raise Denial("unsupported")
        if operation in ("exclusion", "admission"):
            return super().validate_action(action)
        if operation == "workload":
            preset_for(action)
        elif (action["fault"] not in ALLOWED or not 1 <= action["duration_ms"] <= MAX_DURATION_MS
              or action["resource"]["id"].removeprefix("resource-") not in ALLOWED[action["fault"]]):
            raise Denial("unsupported")

    def _apply_reserved(self, packet, deployment, session, origin, recheck=None):
        operation = packet["action"]["operation"]
        if operation in ("exclusion", "admission"):
            return super()._apply_reserved(packet, deployment, session, origin, recheck)
        operation_id, identity = packet["operation_id"], packet["deployment"]
        def authorize():
            if self.failed.is_set():
                raise Denial("disconnected")
            if recheck is not None:
                recheck()
            else:
                authorize_command(packet, deployment, session, origin)
        def admitted():
            self.ledger.advance(operation_id, identity, "pending")
        try:
            if operation == "fault":
                def authorize_fault():
                    authorize()
                    admitted()
                self.faults.launch(operation_id, packet["action"], authorize_fault)
            elif operation == "workload":
                self.workload.run(operation_id, identity, authorize, admitted, self.failed.is_set)
            else:
                raise Denial("unsupported")
            return self._reconcile(operation_id, identity)
        except Denial as error:
            result = self.ledger.get(operation_id, identity)
            if result["status"] in TERMINAL:
                return result
            return self.ledger.advance(
                operation_id, identity, "rejected" if result["status"] == "requested" else "unknown",
                error_code=operation_error(error.code))
        except EFFECT_UNCERTAIN:
            return self.ledger.advance(operation_id, identity, "unknown", error_code="disconnected")
        finally:
            self.lock.release()

    def _reconcile(self, operation_id, deployment, expected=None):
        result = self.ledger.get(operation_id, deployment)
        if result["status"] in TERMINAL:
            return result
        intent = self.ledger.intent(operation_id, deployment)
        operation = intent["action"]["operation"]
        if operation in ("exclusion", "admission"):
            return super()._reconcile(operation_id, deployment, expected)
        try:
            if operation == "fault" and self.faults is not None:
                return self._fault_outcome(operation_id, deployment, intent)
            if operation == "workload" and self.workload is not None:
                status, revision = self.workload.outcome(operation_id, deployment)
                return self.ledger.advance(operation_id, deployment, status, revision,
                                           error_code="unknown" if status == "unknown" else None)
            raise Denial("unsupported")
        except Denial as error:
            return self.ledger.advance(operation_id, deployment, "unknown", error_code=operation_error(error.code))
        except EFFECT_UNCERTAIN:
            return self.ledger.advance(operation_id, deployment, "unknown", error_code="disconnected")

    def _fault_outcome(self, operation_id, deployment, intent):
        record = self.faults.journal.get(operation_id)
        if record["intent"] != {"deployment": deployment, "action": intent["action"]}:
            raise Denial("conflict")
        if record["phase"] == "rejected":
            result = self.ledger.get(operation_id, deployment)
            return self.ledger.advance(
                operation_id, deployment, "rejected" if result["status"] == "requested" else "failed",
                error_code=operation_error(record["error_code"] or "failed"))
        if record["phase"] in ("requested", "pause-sent", "paused", "rollback-pending"):
            return self.ledger.advance(operation_id, deployment, "pending")
        if record["phase"] != "repaired" or not record["applied"]:
            return self.ledger.advance(operation_id, deployment, "unknown", error_code="unknown")
        state = exact_state(self.faults.runtime, record)
        if not state["Running"] or state.get("Paused"):
            raise Denial("disconnected")
        before_snapshot = time.monotonic_ns()
        snapshot = validate_packet(self.snapshot())
        freshness = snapshot["freshness"]
        if (snapshot["deployment"] != deployment or freshness["status"] != "fresh"
                or freshness["age_ns"] is None or int(freshness["age_ns"]) > SNAPSHOT_AGE_NS):
            raise Denial("stale")
        if (record.get("repair_boot") != boot_identity() or record.get("repaired_ns") is None
                or before_snapshot - int(freshness["age_ns"]) < record["repaired_ns"]):
            return self.ledger.advance(operation_id, deployment, "pending")
        replica = next((item for item in snapshot["replicas"]
                        if item["resource"] == intent["action"]["resource"]), None)
        if replica is None or replica["status"] in ("failed", "retired"):
            raise Denial("wrong-target")
        if replica["role"] in ("collector", "gateway") and replica["status"] != "active":
            raise Denial("stale")
        return self.ledger.advance(operation_id, deployment, "completed", snapshot["revision"])
