"""Locust scheduling with explicitly native, bounded Flight workers."""

import os
import time

import gevent
from gevent.lock import BoundedSemaphore
from gevent.threadpool import ThreadPool
from locust import User, between, events, task
from locust.exception import StopUser

from native import upload
from workload import save_report, settings_from_file

state = {}


@events.test_start.add_listener
def start(environment, **_):
    settings, sdks = settings_from_file(os.environ["ARROWCOL_WORKLOAD_SETTINGS"])
    if environment.parsed_options.num_users > settings["workers"]:
        raise ValueError("Locust users must not exceed the native worker bound")
    state.update(settings=settings, sdks=sdks, ordinal=0, outcomes=[], started=time.monotonic(),
                 pool=ThreadPool(settings["workers"]), permits=BoundedSemaphore(settings["workers"]))


@events.test_stop.add_listener
def stop(environment, **_):
    if not state:
        return
    with gevent.Timeout(state["settings"]["timeout"] + 5, False):
        state["pool"].join()
    if len(state["pool"]):
        environment.process_exit_code = 2
        return
    report = save_report(state["settings"], state["outcomes"], state["started"])
    if not report["successful_pilot"]:
        environment.process_exit_code = 1
    state["pool"].kill()


class NativeFlightUser(User):
    wait_time = between(0, 0)

    @task
    def stream(self):
        ordinal = state["ordinal"]
        if time.monotonic() - state["started"] >= state["settings"]["duration_seconds"]:
            if len(state["outcomes"]) == ordinal:
                self.environment.runner.quit()
            raise StopUser()
        if ordinal >= state["settings"]["streams"]:
            if len(state["outcomes"]) == ordinal:
                self.environment.runner.quit()
            raise StopUser()
        state["ordinal"] += 1
        with state["permits"]:
            outcome = state["pool"].spawn(
                upload, state["settings"], state["sdks"][ordinal % len(state["sdks"])], ordinal
            ).get()
        state["outcomes"].append(outcome)
        self.environment.events.request.fire(
            request_type="Flight", name="DoPut terminal (not publication)",
            response_time=outcome["elapsed_ms"], response_length=outcome["arrow_bytes"],
            exception=None if outcome["terminal"] == "ok" else RuntimeError(outcome.get("error", "unknown")),
            context={"stream_id": outcome["stream_id"], "receipts": len(outcome["receipts"])},
        )
        if len(state["outcomes"]) >= state["settings"]["streams"]:
            self.environment.runner.quit()
