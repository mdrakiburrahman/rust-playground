"""Bounded, read-only task observations; never an admission or replay authority."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import http.client
import json
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
import urllib.parse


class ClientAccounting:
    """Logical Arrow bytes, not allocator RSS or gRPC/native buffers."""
    def __init__(self, jobs, design):
        if not 0 <= jobs <= 256:
            raise ValueError("observation job bound")
        self.lock = threading.Lock()
        self.queued = jobs
        self.design = design
        self.units = {}
        self.peaks = {"retained_bytes": 0, "in_flight_bytes": 0, "active_units": 0}

    def _update(self):
        values = self._values()
        for key in self.peaks:
            self.peaks[key] = max(self.peaks[key], values[key])

    def _values(self):
        return {"queued_jobs": self.queued, "active_units": sum(u["active"] for u in self.units.values()),
                "retained_bytes": sum(u["retained"] for u in self.units.values()),
                "in_flight_bytes": sum(u["flight"] for u in self.units.values())}

    def begin(self, key):
        with self.lock:
            if key in self.units or self.queued <= 0:
                raise ValueError("duplicate/unsubmitted observation unit")
            self.queued -= 1
            self.units[key] = {"active": True, "retained": 0, "flight": 0}
            self._update()

    def retain(self, key, amount):
        if not 0 <= amount <= 8 * 1024 * 1024:
            raise ValueError("retained observation byte bound")
        with self.lock:
            self.units[key]["retained"] = amount
            self._update()

    def attempt(self, key, active):
        with self.lock:
            self.units[key]["flight"] = self.units[key]["retained"] if active else 0
            self._update()

    def baseline_batch(self, key, amount):
        with self.lock:
            self.units[key]["flight"] = amount
            self._update()

    def complete(self, key, success):
        with self.lock:
            unit = self.units[key]
            unit["active"] = False
            unit["flight"] = 0
            if success:
                unit["retained"] = 0
            self._update()

    def sample(self):
        with self.lock:
            return {**self._values(), "high_water": dict(self.peaks),
                    "unresolved_retained_bytes": sum(u["retained"] for u in self.units.values() if not u["active"]),
                    "design": self.design}


class Engine:
    def __init__(self):
        endpoint = json.loads(subprocess.check_output(["docker", "--context", "default", "context", "inspect",
            "--format", "{{json .Endpoints.docker.Host}}"], text=True))
        if not endpoint.startswith("unix://"):
            raise ValueError("observation requires the existing default Unix Docker endpoint")
        self.path = endpoint.removeprefix("unix://")

    def get(self, path):
        connection = http.client.HTTPConnection("localhost", timeout=2)
        connection.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.sock.settimeout(2)
        try:
            connection.sock.connect(self.path)
            connection.request("GET", "/v1.52" + path)
            response = connection.getresponse()
            raw = response.read(2 * 1024 * 1024 + 1)
            if response.status != 200 or len(raw) > 2 * 1024 * 1024:
                raise RuntimeError(f"bounded Docker read failed: HTTP {response.status}")
            return json.loads(raw)
        finally:
            connection.close()


def read_ns(value):
    value = value.replace("Z", "+00:00")
    whole, separator, fraction = value.partition(".")
    nanos = 0
    if separator:
        match = re.fullmatch(r"(\d{1,9})([+-]\d\d:\d\d)", fraction)
        if match is None:
            raise ValueError("invalid bounded Engine timestamp")
        value = whole + match[2]
        nanos = int(match[1].ljust(9, "0"))
    return int(datetime.fromisoformat(value).timestamp()) * 1_000_000_000 + nanos


def cpu_interval(previous, current):
    if previous is None:
        return None
    elapsed = current["engine_read_ns"] - previous["engine_read_ns"]
    used = current["cpu_total_usage_ns"] - previous["cpu_total_usage_ns"]
    if elapsed <= 0 or used < 0:
        return None
    return {"elapsed_ns": elapsed, "cpu_usage_ns": used, "one_core_percent": 100 * used / elapsed}


class Observer:
    def __init__(self, project, directory, cadence=0.5, probe=None, label_namespace="com.docker.compose.project"):
        if not (project.startswith("adr0001-") or project.startswith("pingora-bench-adr")
                or project.startswith("publication-gate-")):
            raise ValueError("unowned observation project")
        if label_namespace not in ("com.docker.compose.project", "delta-runtime"):
            raise ValueError("unsupported observation label namespace")
        self.label_namespace = label_namespace
        if not 0.5 <= cadence <= 5:
            raise ValueError("observation cadence bound")
        self.project = project
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.engine = Engine()
        self.cadence = cadence
        self.probe = probe
        self.last_probe_ns = 0
        self.lock = threading.Lock()
        self.current = "startup"
        self.client = None
        self.intervals = []
        self.stop_event = threading.Event()
        self.previous = {}
        self.samples = 0
        self.errors = 0
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.output = (self.directory / "samples.jsonl").open("w")

    def phase(self, label, jobs=0, design=None):
        with self.lock:
            self.current = label
            self.client = ClientAccounting(jobs, design) if design else None
            self.intervals.append({"label": label, "started_ns": time.time_ns(), "design": design})
            return self.client

    def finish(self, label):
        with self.lock:
            interval = next(v for v in reversed(self.intervals) if v["label"] == label)
            interval["ended_ns"] = time.time_ns()
            if self.client is not None:
                interval["client_final"] = self.client.sample()
                self.client = None
            self.current = "between-intervals"

    def _container(self, row):
        identifier = row["Id"]
        if row["Labels"].get(self.label_namespace) != self.project:
            raise ValueError("Docker topology scope mismatch")
        raw = self.engine.get(f"/containers/{identifier}/stats?stream=false&one-shot=true")
        memory = raw.get("memory_stats", {})
        details = memory.get("stats", {})
        return {"id": identifier, "name": row["Names"][0].lstrip("/"),
                "service": row["Labels"].get("com.docker.compose.service",
                    row["Names"][0].lstrip("/").removeprefix(self.project + "-")), "image_id": row["ImageID"],
                "engine_read_utc": raw["read"],
                "engine_read_ns": read_ns(raw["read"]),
                "cpu_total_usage_ns": raw["cpu_stats"]["cpu_usage"]["total_usage"],
                "memory_usage_bytes": memory.get("usage"), "memory_limit_bytes": memory.get("limit"),
                "cgroup_rss_or_anon_bytes": details.get("rss", details.get("anon")),
                "pids": raw.get("pids_stats", {}).get("current")}

    def sample(self):
        started = time.time_ns()
        with self.lock:
            phase = self.current
            client = self.client.sample() if self.client else None
        record = {"started_ns": started, "project": self.project, "phase": phase, "client": client}
        try:
            query = urllib.parse.urlencode({"filters": json.dumps({
                "label": [self.label_namespace + "=" + self.project]})})
            topology = self.engine.get("/containers/json?" + query)
            if len(topology) > 16:
                raise ValueError("observation topology bound")
            components = list(self.executor.map(self._container, topology))
            for component in components:
                component["cpu_interval"] = cpu_interval(self.previous.get(component["id"]), component)
                self.previous[component["id"]] = component
            live_ids = {c["id"] for c in components}
            self.previous = {k: v for k, v in self.previous.items() if k in live_ids}
            record["containers"] = components
            if self.probe is not None and started - self.last_probe_ns >= 1_000_000_000:
                record["control_observation"] = self.probe()
                self.last_probe_ns = started
        except Exception as error:
            self.errors += 1
            record["error"] = type(error).__name__ + ": " + str(error)[:300]
        record["ended_ns"] = time.time_ns()
        self.output.write(json.dumps(record) + "\n")
        self.output.flush()
        self.samples += 1

    def start(self):
        self.started_ns = time.time_ns()
        self.sample()
        if self.errors:
            self.executor.shutdown(wait=True)
            self.output.close()
            raise RuntimeError("initial observation failed; sample evidence preserved")
        def loop():
            while not self.stop_event.wait(self.cadence):
                if self.samples >= 1800 or time.time_ns() - self.started_ns >= 900_000_000_000:
                    self.errors += 1
                    return
                self.sample()
        self.thread = threading.Thread(target=loop, name="task-observation", daemon=False)
        self.thread.start()
        return self

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=30)
        if self.thread.is_alive():
            raise RuntimeError("bounded observer did not stop")
        self.sample()
        self.executor.shutdown(wait=True)
        self.output.close()
        summary = {"project": self.project, "started_ns": self.started_ns, "ended_ns": time.time_ns(),
                   "cadence_seconds": self.cadence, "max_samples": 1800, "max_seconds": 900,
                   "samples": self.samples, "sampling_errors": self.errors, "intervals": self.intervals,
                   "semantics": "Point cgroup memory samples and CPU counter deltas over recorded engine-read intervals; "
                     "not continuous maxima, process RSS, exact consensus-event latency or a capacity estimate."}
        (self.directory / "intervals.json").write_text(json.dumps(summary, indent=2))
        if self.errors:
            raise RuntimeError("observation sampling errors retained in samples.jsonl")
