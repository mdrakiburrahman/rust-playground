"""Finite authenticated runtime correctness, not performance or report generation.

Reuses the retained publication gate's safe named SQL lifecycle and evidence
writer. Every invocation requires a new project/subnet. No deletion/pruning.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time
import urllib.request
import urllib.error
import uuid

import pyarrow as pa
import pyarrow.flight as flight

from publication_gate import Gate, REPO, RUNTIME_IMAGE
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "routing-benchmark"))
import native


class Runtime(Gate):
    def __init__(self, project, subnet, arm, backend, legacy=False):
        super().__init__(project, subnet)
        self.arm, self.backend = arm, backend
        self.legacy = legacy
        self.names = {}
        self.expected = {"main": [], "dictionary": []}
        self.uploads = []
        self.executor = ThreadPoolExecutor(max_workers=4)
        sources = []
        for base in ["crates", "projects/arrowcol-router-rust"]:
            sources += [p for p in (REPO / base).rglob("*") if p.is_file() and p.suffix in (".rs", ".toml", ".sql")]
        sources += [REPO / "Cargo.toml", REPO / "Cargo.lock", Path(__file__)]
        self.source_hashes = {
            str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(set(sources))
        }
        (self.root / "runtime-sources-at-start.json").write_text(json.dumps(self.source_hashes, indent=2))
        for directory in ("bin", "config", "staging", "delta-private/tables", "faults", "router-state"):
            (self.root / directory).mkdir(parents=True, exist_ok=True)
        self.runtime_password = self.root / "config/runtime-sql-password"
        self.runtime_password.write_text(secrets.token_hex(32) + "aA1!\n")
        self.runtime_password.chmod(0o600)
        self.schemas = [
            pa.schema([pa.field("id", pa.int64(), nullable=False)]),
            pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("added", pa.string())]),
            pa.schema([pa.field("id", pa.int64(), nullable=False),
                       pa.field("tag", pa.dictionary(pa.int8(), pa.string()), nullable=False)]),
        ]
        self.fingerprints = []
        for n, schema in enumerate(self.schemas):
            path = self.root / f"schema-{n}.arrow"
            with pa.OSFile(str(path), "wb") as stream, pa.ipc.new_stream(stream, schema):
                pass
            self.fingerprints.append(self.run([REPO / "target/debug/adr-fingerprint", path]).stdout.strip())

    def start_role(self, role, binary, extra=(), private=False, aliases=(), ports=(), env=(), stage=False):
        name = f"{self.project}-{role}"
        command = ["docker", "run", "-d", "--name", name, "--label", f"delta-runtime={self.project}",
                   "--network", self.project, "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges",
                   "--user", f"{21001 if stage else os.getuid()}:{21001 if stage else os.getgid()}",
                   "--cpus", "1", "--memory", "1g", "--pids-limit", "256",
                   "-e", "ARROWCOL_TRACE=1",
                   "--mount", f"type=bind,src={self.root/'bin'},dst=/native,readonly",
                   "--mount", f"type=bind,src={self.root/'config'/f'{role}.json'},dst=/config.json,readonly"]
        for alias in aliases:
            command += ["--network-alias", alias]
        for port in ports:
            command += ["-p", f"127.0.0.1::{port}"]
        for value in env:
            command += ["-e", value]
        if private:
            command += ["--mount", f"type=bind,src={self.root/'config/private.json'},dst=/private.json,readonly",
                        "--mount", f"type=bind,src={self.runtime_password},dst=/sql-password,readonly"]
        if self.legacy:
            command += ["--mount", f"type=bind,src={self.root/'legacy-objects'},dst=/objects"]
        if stage:
            directory = self.root / "staging" / role
            directory.mkdir(exist_ok=True)
            directory.chmod(0o777)
            command += ["--mount", f"type=bind,src={directory},dst=/stage",
                        "--mount", f"type=bind,src={self.root/'delta-private/tables'},dst=/destination,readonly"]
        if role == "publisher":
            command += ["--mount", f"type=bind,src={self.root/'delta-private'},dst=/private",
                        "--mount", f"type=bind,src={self.root/'staging'},dst=/staging,readonly",
                        "--mount", f"type=bind,src={self.root/'faults'},dst=/faults",
                        "-e", "ARROWCOL_DELTA_FAULTS=/faults"]
        if role.startswith("gateway"):
            state = self.root / ("router-state" if role == "gateway" else f"router-state-{role}")
            state.mkdir(exist_ok=True)
            command += ["--mount", f"type=bind,src={state},dst=/runtime"]
        command += [RUNTIME_IMAGE, "/native/" + binary, *extra]
        self.run(command)
        self.names[role] = name
        self.containers.append(name)

    def port_of(self, role, port):
        return self.run(["docker", "port", self.names[role], f"{port}/tcp"]).stdout.strip().split(":")[-1]

    def setup_runtime(self):
        super().setup()
        # Retain, but stop, the gate fixture. New collectors below run the actual
        # Flight binary with their own restricted mounts and scoped credentials.
        self.run(["docker", "stop", "-t", "1", self.collector])
        for binary in ("adr-controller", "adr-flight", "adr-publisher", "arrowcol-router-rust", "delta-contract"):
            shutil.copy2(REPO / "target/debug" / binary, self.root / "bin" / binary)
        # Administrative bootstrap only. Runtime uses a database-scoped login,
        # never sa; SQL credentials never enter a collector configuration/mount.
        statement = """CREATE DATABASE delta_runtime;
GO
CREATE LOGIN delta_runtime WITH PASSWORD = 'RUNTIME_PASSWORD';
GO
USE delta_runtime;
CREATE USER delta_runtime FOR LOGIN delta_runtime;
ALTER ROLE db_owner ADD MEMBER delta_runtime;
"""
        statement = statement.replace("RUNTIME_PASSWORD", self.runtime_password.read_text().strip())
        result = subprocess.run(["docker", "exec", "-i", self.sql_name, "bash", "-c",
            'export SQLCMDPASSWORD="$(cat /run/secrets/sqlserver_sa_password)"; '
            'exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b'],
            input=statement, text=True, capture_output=True)
        assert result.returncode == 0, result.stderr
        probe = subprocess.run(["docker", "exec", "-i", self.sql_name, "bash", "-c",
            "IFS= read -r SQLCMDPASSWORD; export SQLCMDPASSWORD; "
            "exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U delta_runtime -d delta_runtime -C -b -h-1 -W "
            """-Q "SET NOCOUNT ON; SELECT ORIGINAL_LOGIN(),IS_SRVROLEMEMBER('sysadmin'),DB_NAME()" """],
            input=self.runtime_password.read_text(), text=True, capture_output=True)
        assert probe.returncode == 0, probe.stderr
        assert probe.stdout.split() == ["delta_runtime", "0", "delta_runtime"], probe.stdout
        assert self.runtime_password.read_text() != self.password.read_text()
        (self.root / "runtime-sql-identity.json").write_text(json.dumps({
            "login": "delta_runtime", "sysadmin": False, "database": "delta_runtime",
            "independent_password": True,
        }, indent=2))
        self.mark("runtime SQL login is not sysadmin and has a distinct bootstrap password")
        etcd_password = secrets.token_hex(32)
        self.config = {
            "delta": {"publisher": "http://publisher:50061"},
            "tenant": "runtime-test", "catalog_id": "immutable-catalog",
            "tables": [
                {"key": "main", "path": ["telemetry", "runtime", "main"], "fingerprints": self.fingerprints[:2]},
                {"key": "dictionary", "path": ["telemetry", "runtime", "dictionary"], "fingerprints": self.fingerprints[2:]},
                {"key": "cas", "path": ["telemetry", "runtime", "cas"], "fingerprints": self.fingerprints[:1]},
            ],
            "storage": "/stage", "controller": "http://control:50060",
            "operator_token": secrets.token_hex(32), "gateway_token": secrets.token_hex(32),
            "edge_secret": secrets.token_hex(32), "internal_secret": secrets.token_hex(32),
            "collectors": {f"collector{i}": secrets.token_hex(32) for i in range(1, 5)},
            "etcd": ["http://etcd:2379"], "etcd_user": "runtime", "etcd_password": etcd_password,
        }
        private = {"backend": self.backend, "sql_host": self.sql_name, "sql_port": 1433,
                   "sql_database": "delta_runtime", "sql_user": "delta_runtime",
                   "sql_password_file": "/sql-password", "root": "/private", "staging": "/staging"}
        if self.legacy:
            self.config.pop("delta")
            self.config["storage"] = "/objects"
            (self.root / "legacy-objects").mkdir()
            (self.root / "legacy-objects").chmod(0o777)
        (self.root / "config/private.json").write_text(json.dumps(private))
        (self.root / "config/private.json").chmod(0o600)
        for role in ["controller", "publisher", "gateway", *self.config["collectors"]]:
            scoped = dict(self.config)
            if role.startswith("collector") or (role == "gateway" and self.arm == "adr"):
                scoped.update(etcd=[], etcd_user="", etcd_password="", operator_token="")
                if role.startswith("collector"):
                    scoped.update(gateway_token="", collectors={role: self.config["collectors"][role]})
                else:
                    scoped["collectors"] = {}
            path = self.root / "config" / f"{role}.json"
            path.write_text(json.dumps(scoped))
            path.chmod(0o444 if role.startswith("collector") else 0o600)
        if self.backend == "etcd":
            name = self.project + "-etcd"
            self.run(["docker", "volume", "create", name])
            self.run(["docker", "run", "-d", "--name", name, "--network", self.project,
                      "--network-alias", "etcd", "--cpus", "1", "--memory", "512m",
                      "--mount", f"type=volume,src={name},dst=/data",
                      "registry.k8s.io/etcd:3.6.4-0", "/usr/local/bin/etcd",
                      "--data-dir=/data", "--listen-client-urls=http://0.0.0.0:2379",
                      "--advertise-client-urls=http://etcd:2379"])
            self.containers.append(name)
            self.names["etcd"] = name
            time.sleep(2)
            for args in [
                ["user", "add", "root", "--new-user-password", secrets.token_hex(32)],
                ["user", "grant-role", "root", "root"],
                ["user", "add", "runtime", "--new-user-password", etcd_password],
                ["role", "add", "runtime"],
                ["role", "grant-permission", "runtime", "readwrite", "/adr/" if self.legacy else "/delta/", "--prefix"],
                ["user", "grant-role", "runtime", "runtime"],
                ["auth", "enable"],
            ]:
                # Bootstrap credentials never enter retained argv logs.
                r = subprocess.run(["docker", "exec", name, "/usr/local/bin/etcdctl", *args],
                                   text=True, capture_output=True)
                assert r.returncode == 0, r.stderr
        args = ["--config", "/config.json", "--private", "/private.json"]
        if self.arm == "adr":
            self.start_role("controller", "adr-controller", ["--config", "/config.json"] if self.legacy else args,
                            private=not self.legacy, aliases=["control"], ports=[50060])
            self.control_role = "controller"
        else:
            self.start_role("gateway", "arrowcol-router-rust", private=True, aliases=["control"], ports=[50060, 50051],
                            env=["ARROWCOL_DELTA_CONFIG=/config.json", "ARROWCOL_DELTA_PRIVATE=/private.json"])
            self.control_role = "gateway"
        self.control_port = self.port_of(self.control_role, 50060)
        self.wait(lambda: self.post({"op": "view"}))
        if not self.legacy:
            self.start_role("publisher", "adr-publisher", args, private=True, aliases=["publisher"], ports=[50061])
            self.publisher_port = self.port_of("publisher", 50061)
        for role in ("collector1", "collector2", "collector3"):
            self.start_role(role, "adr-flight", ["--config", "/config.json", "--collector", role],
                            aliases=[role], stage=True)
        if self.arm == "adr":
            self.start_role("gateway", "adr-flight", ["--config", "/config.json"], ports=[50051])
        self.gateway_port = self.port_of("gateway", 50051)
        self.wait(lambda: len(self.post({"op": "view"})["members"]) == 3)
        if not self.legacy:
            self.wait(lambda: len(self.post({"op": "view"}).get("observations", {})) == 3)
        self.mark("authenticated runtime ready", arm=self.arm, authority=self.backend,
                  collector_binary=hashlib.sha256((self.root / "bin/adr-flight").read_bytes()).hexdigest())

    def post(self, operation, principal="operator", publisher=False, ok=True):
        token = self.config.get(principal + "_token", self.config["collectors"].get(principal, principal))
        port = self.publisher_port if publisher else self.control_port
        request = urllib.request.Request(f"http://127.0.0.1:{port}/v1/authority",
            data=json.dumps(operation).encode(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                value = json.load(response)
            assert ok, ("unexpected authority success", operation, value)
            return value
        except urllib.error.HTTPError as error:
            value = json.loads(error.read())
            if ok:
                raise RuntimeError(value)
            return value

    def wait(self, check, seconds=45):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                value = check()
                if value:
                    return value
            except Exception as error:
                last = str(error)
            time.sleep(0.2)
        raise AssertionError(("bounded runtime wait expired", last))

    def assignment(self, key="main"):
        return self.post({"op": "resolve", "key": key}, "gateway")

    def proof_alignment(self):
        assert not self.legacy
        first = self.assignment()
        assert first["record"]["lifecycle"] == "Recovering", first
        active = self.wait(lambda: (a if (a := self.assignment())["record"]["lifecycle"] == "Active" else None))
        owner = active["record"]["writer"]
        self.upload(2000000, half_close=True)
        self.upload(2000010, schema_index=1, half_close=True)
        assert self.assignment()["record"]["writer"] == owner
        self.upload(2000020, key="cas", half_close=True)
        assert self.assignment("cas")["record"]["key"] != self.assignment()["record"]["key"]
        self.mark("cold recovery, schema evolution and independent same-schema table use real native Delta")

        def policy(excluded, blocked, retiring=False):
            state = self.post({"op": "view"})
            previous = state.get("policies", {}).get(owner["collector"], {})
            result = self.post({"op": "policy", "collector": owner["collector"], "writer": owner,
                               "revision": previous.get("revision", 0), "operation": str(uuid.uuid4()),
                               "excluded": excluded, "block_streams": blocked, "retiring": retiring})
            self.wait(lambda: self.post({"op": "view"})["policies"][owner["collector"]]["applied_revision"] == result["revision"])
            return result

        policy(True, False)
        self.upload(2000030, schema_index=1, half_close=True)
        assert self.assignment()["record"]["writer"] == owner
        policy(True, True)
        rejected = self.upload(2000040, schema_index=1, half_close=True, expect=False)
        assert rejected["terminal"] != "final" and not any(r["kind"] == "DELTA_COMMITTED" for r in rejected["receipts"])
        assert self.assignment()["record"]["writer"] == owner
        policy(False, False)
        self.upload(2000050, schema_index=1, half_close=True)
        self.mark("placement exclusion preserves owner streams; separately applied block rejects new native streams")

        retiring = policy(True, False, True)
        approval = self.wait(lambda: (p if (p := self.post({"op": "view"})["policies"][owner["collector"]])["approved"] else None))
        assert approval["incarnation"] == owner and approval["operation"] == retiring["operation"]
        assert not approval["recovery_obligations"]
        state = self.post({"op": "view"})
        assert all(a["record"]["writer"] != owner and a["record"]["lifecycle"] == "Active"
                   for a in state["assignments"])
        self.run(["docker", "stop", "-t", "1", self.names[owner["collector"]]])
        self.upload(2000060, schema_index=1, half_close=True)
        assert self.assignment()["record"]["epoch"] > active["record"]["epoch"]
        assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
        assert self.scan_runtime("cas")["rows"] == sorted(self.expected["cas"])
        self.mark("exact-incarnation retirement waits for recovered successors before teardown and preserves Delta rows")

    def upload(self, first, schema_index=0, extra_headers=None, expect=True, *,
               count=3, port=None, key=None, half_close=False):
        key = key or ("dictionary" if schema_index == 2 else "main")
        schema = self.schemas[schema_index]
        headers = {"content-type": "application/grpc", "x-arrowcol-protocol-version": "1",
                   "x-arrowcol-implementation-pool": "direct", "x-arrowcol-catalog": "telemetry",
                   "x-arrowcol-schema": "runtime", "x-arrowcol-table": key,
                   "x-arrowcol-schema-fingerprint": self.fingerprints[schema_index]}
        headers = native.signed_headers(self.config["edge_secret"].encode(), "POST", native.PUT_PATH, headers)
        headers.update(extra_headers or {})
        arrays = [pa.array(range(first, first + count), type=pa.int64())]
        if schema_index == 1:
            arrays.append(pa.array(["evolved"] * count))
        if schema_index == 2:
            arrays.append(pa.array((["one", "two", "one"] * ((count + 2) // 3))[:count],
                                   type=schema.field("tag").type))
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        outcome = {"first": first, "rows": count, "table": key, "receipts": [], "terminal": "unknown",
                   "gateway_port": port or self.gateway_port, "half_close": half_close}
        self.uploads.append(outcome)
        client = flight.FlightClient(f"grpc://127.0.0.1:{port or self.gateway_port}")
        writer = None
        try:
            writer, reader = client.do_put(
                flight.FlightDescriptor.for_path("telemetry", "runtime", key, "v2" if schema_index == 1 else "v1"),
                schema, flight.FlightCallOptions(timeout=25, headers=[(k.encode(), v.encode()) for k, v in headers.items()]))
            if count:
                writer.write_batch(batch)
            if half_close:
                writer.done_writing()
            while True:
                raw = reader.read()
                if raw is None:
                    if count == 0:
                        break
                    raise RuntimeError("ended without final Delta receipt")
                receipt = json.loads(raw.to_pybytes())
                outcome["receipts"].append(receipt)
                if receipt["kind"] == ("TABLE_COMMITTED" if self.legacy else "DELTA_COMMITTED"):
                    break
                assert receipt["kind"] == "DELTA_OPERATION" and receipt["final"] is False
            if not half_close:
                writer.done_writing()
            while reader.read() is not None:
                pass
            writer.close()
            writer = None
            outcome["terminal"] = "final" if count else "empty"
        except Exception as error:
            outcome["error"] = str(error)[:2000]
        finally:
            if writer:
                try:
                    writer.close()
                except Exception as error:
                    outcome["terminal_close_error"] = str(error)[:2000]
            client.close()
            (self.root / "uploads.json").write_text(json.dumps(self.uploads, indent=2))
        if expect:
            assert outcome["terminal"] == ("final" if count else "empty"), outcome
            self.expected.setdefault(key, []).extend(range(first, first + count))
        return outcome

    def proof_gateway_streams(self, reproduce=False):
        self.upload(0, half_close=True)
        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(self.upload, 10000 + index * 65536, count=65536,
                                   half_close=True, expect=not reproduce) for index in range(8)]
            results = [future.result() for future in futures]
        (self.root / "multiplexed-streams.json").write_text(json.dumps(results, indent=2))
        if reproduce:
            assert any(result["terminal"] != "final" for result in results)
            self.mark("old multiplexed native stream failure reproduced without payload retry")
            return
        assert all(result["terminal"] == "final" for result in results)
        assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
        self.mark("eight half-closed 65536-row native inputs at concurrency four have final frames and exact Delta rows")
        self.proof_data_only_gateway()

    def proof_data_only_gateway(self):
        scoped = dict(self.config)
        scoped.update(etcd=[], etcd_user="", etcd_password="", operator_token="", collectors={})
        path = self.root / "config/gateway2.json"
        path.write_text(json.dumps(scoped))
        path.chmod(0o600)
        if self.arm == "pingora":
            self.start_role("gateway2", "arrowcol-router-rust", ports=[50051], env=[
                "ARROWCOL_DELTA_CONFIG=/config.json", "ARROWCOL_DELTA_MODE=data_only"])
        else:
            self.start_role("gateway2", "adr-flight", ["--config", "/config.json"], ports=[50051])
        second = self.port_of("gateway2", 50051)
        self.wait(lambda: self.run(["docker", "inspect", "-f", "{{.State.Running}}",
                                   self.names["gateway2"]]).stdout.strip() == "true")
        self.run(["docker", "exec", self.names["gateway2"], "sh", "-c",
                  "test ! -e /private.json && test ! -e /sql-password"])
        with ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(self.upload, 1000000 + index * 3, key="cas",
                                port=port, half_close=True) for index, port in enumerate([second, self.gateway_port])]
            for job in jobs:
                job.result()
        grant = self.assignment("cas")
        assert grant["record"]["epoch"] == 1
        self.mark("gateways 1-to-2: actual data-only process, cold shared assignment and native finals through both")
        self.upload(1100000, key="cas", port=second, half_close=True)
        if self.arm == "pingora":
            # A second authoritative process must still fail; data-only is not
            # an election fallback or relaxed controller CAS.
            (self.root / "config/gateway-contender.json").write_text(json.dumps(self.config))
            self.start_role("gateway-contender", "arrowcol-router-rust", private=True, env=[
                "ARROWCOL_DELTA_CONFIG=/config.json", "ARROWCOL_DELTA_PRIVATE=/private.json"])
            self.wait(lambda: self.run(["docker", "inspect", "-f", "{{.State.Running}}",
                                       self.names["gateway-contender"]]).stdout.strip() == "false")
            assert "controller already live" in self.run(
                ["docker", "logs", self.names["gateway-contender"]]).stderr
            self.mark("authoritative Pingora replica still rejected by exclusive controller fencing")
        self.upload(0, count=0, key="cas", port=second, half_close=True)
        outage_ms = int(time.time() * 1000)
        authority = self.sql_name if self.backend == "sql" else self.names["etcd"]
        self.run(["docker", "pause", authority])
        try:
            rejected = self.upload(2000000, key="cas", port=second, half_close=True, expect=False)
            assert rejected["terminal"] != "final"
            assert not any(x.get("kind") == "DELTA_COMMITTED" for x in rejected["receipts"])
        finally:
            self.run(["docker", "unpause", authority])
        self.wait(lambda: self.assignment("cas")["record"]["lifecycle"] == "Active")
        if self.arm == "pingora":
            text = self.run(["docker", "logs", self.names["gateway2"]]).stderr
            events = [json.loads(line) for line in text.splitlines() if '"event":"delta_route"' in line]
            assert any(event["observed_ms"] >= outage_ms and event["cache_hit"] for event in events), events
        self.mark("actual warm data-only cache cannot authorize publication during authority outage")
        self.upload(1200000, key="cas", half_close=True)
        self.run(["docker", "pause", self.names[self.control_role]])
        try:
            time.sleep(0.4)
            rejected = self.upload(2100000, port=second, key="cas", half_close=True, expect=False)
            assert rejected["terminal"] != "final" and not rejected["receipts"], rejected
        finally:
            self.run(["docker", "unpause", self.names[self.control_role]])
        self.wait(lambda: self.assignment("cas")["record"]["epoch"] == grant["record"]["epoch"])
        self.mark("data-only fails closed when primary private control is unavailable and does not elect itself")
        self.run(["docker", "stop", "-t", "1", self.names["gateway2"]])
        self.upload(1300000, key="cas", half_close=True)
        self.mark("gateways 2-to-1: only data-only stopped; original primary still publishes")
        # A longer accepted prefix can exceed the unchanged publisher HTTP
        # deadline. Keep the wire outcome separate from identity reconciliation.
        outcome = self.upload(1400000, half_close=True, expect=False)
        unit = next(r["operation"] for r in outcome["receipts"] if r["kind"] == "DELTA_OPERATION")
        if outcome["terminal"] != "final":
            assert "publisher:50061/v1/authority" in outcome.get("terminal_close_error", ""), outcome
            assert "grpc_status:14" in outcome["terminal_close_error"], outcome
        resolved = self.wait(lambda: self.final(unit))
        assert resolved["operation"] == unit
        self.expected["main"].extend(range(1400000, 1400003))
        (self.root / "post-prefix-outcome.json").write_text(json.dumps({
            "wire": outcome, "identity_resolution": resolved, "payload_replayed": False}, indent=2))
        self.mark("post-prefix wire terminal retained separately from final identity reconciliation",
                  terminal=outcome["terminal"])
        for key in ["main", "cas"]:
            actual = self.scan_runtime(key)["rows"]
            assert actual == sorted(self.expected[key])
        (self.root / "expected-rows.json").write_text(json.dumps(self.expected, indent=2))
        self.mark("focused gateway repair checks passed", arm=self.arm, backend=self.backend, performance=False)

    def scan_runtime(self, key="main"):
        code = ("import json,sys,gc;from deltalake import DeltaTable;"
                "d=DeltaTable(sys.argv[1]);s=d.to_pyarrow_dataset();"
                "t=s.to_table(use_threads=False,batch_readahead=0,fragment_readahead=1);"
                "print(json.dumps({'version':d.version(),'rows':sorted(t['id'].to_pylist()),"
                "'columns':t.column_names,'values':sorted(t.to_pylist(),key=lambda r:r['id'])}));"
                "del t,s,d;gc.collect()")
        return json.loads(self.run([sys.executable, "-c", code, self.root / "delta-private/tables" / key]).stdout)

    def final(self, unit, key="main"):
        result = self.post({"op": "delta_receipt", "key": key, "unit": unit}, publisher=True)
        return result if result["final"] else False

    def barrier(self, phase):
        reached = self.root / "faults" / f"{phase}.reached"
        if reached.exists():
            reached.rename(self.root / "faults" / f"{phase}.{time.time_ns()}.reached")
        (self.root / "faults" / f"{phase}.pause").write_text("bounded correctness fault")

    def reached(self, phase):
        self.wait(lambda: (self.root / "faults" / f"{phase}.reached").exists())

    def release(self, phase):
        (self.root / "faults" / f"{phase}.pause").unlink()

    def proof_runtime(self):
        if self.legacy:
            outcome = self.upload(0)
            head = self.assignment()["record"]["head"]
            manifest = json.loads((self.root / "legacy-objects" / head["manifest"]).read_text())
            import pyarrow.parquet as pq
            rows = pq.read_table(self.root / "legacy-objects" / manifest["parquet"])["id"].to_pylist()
            assert rows == [0, 1, 2] and outcome["receipts"][0]["kind"] == "TABLE_COMMITTED"
            assert not list((self.root / "legacy-objects").rglob("_delta_log"))
            self.mark("legacy omitted-mode native Flight/etcd manifest regression smoke", rows=rows)
            return
        backend_contract = json.loads(self.run(["docker", "exec", self.names[self.control_role],
            "/native/delta-contract", "--config", "/config.json", "--private", "/private.json"]).stdout)
        self.mark("real backend competing claims, controller contention and operation reconciliation", **backend_contract)
        with ThreadPoolExecutor(max_workers=4) as pool:
            assignments = list(pool.map(lambda _: self.assignment(), range(4)))
        assert len({json.dumps(a["record"], sort_keys=True) for a in assignments}) == 1
        first = self.upload(0)
        sticky = self.assignment()
        assert sticky["record"]["writer"] == assignments[0]["record"]["writer"]
        self.upload(10, 2)
        self.upload(20, 1)
        for key in self.expected:
            assert self.scan_runtime(key)["rows"] == sorted(self.expected[key])
        main = self.scan_runtime()
        assert main["columns"] == ["id", "added"]
        assert [r["added"] for r in main["values"]] == [None] * 3 + ["evolved"] * 3
        assert [r["tag"] for r in self.scan_runtime("dictionary")["values"]] == ["one", "two", "one"]
        self.wait(lambda: self.final(first["receipts"][-1]["operation"]))
        self.mark("cold concurrent resolution, sticky native Flight, dictionary, evolution and SQL final receipts")
        members = self.post({"op": "view"})["members"]
        assert all(m["rss_bytes"] > 0 and m["pressure"] > 0 for m in members)
        self.mark("real collector RSS/CPU/active pressure", members=members)
        role = sticky["record"]["writer"]["collector"]
        self.run(["docker", "exec", self.names[role], "sh", "-c",
                  "touch /destination/main/_delta_log/forbidden.json"], ok=False)
        self.run(["docker", "exec", self.names[role], "sh", "-c", "cat /sql-password"], ok=False)
        self.run(["docker", "exec", self.names[role], "sh", "-c",
                  "touch /destination/main/forbidden.parquet"], ok=False)
        config = json.loads((self.root / "config" / f"{role}.json").read_text())
        assert not config["etcd_password"] and not config["operator_token"]
        assert len(config["collectors"]) == 1
        forged = self.upload(900, extra_headers={"x-adr-route": "forged"}, expect=False)
        assert forged["terminal"] != "final"
        self.post({"op": "admit", "key": "main", "writer": sticky["record"]["writer"],
                   "epoch": sticky["record"]["epoch"], "unit": "impersonated"}, principal="collector4", ok=False)
        self.mark("actual collector mount/credential isolation and authenticated identity rejection")
        self.post({"op": "migrate", "key": "main"})
        moved = self.wait(lambda: (a if (a := self.assignment())["record"]["lifecycle"] == "Active"
                         and a["record"]["epoch"] > sticky["record"]["epoch"] else None))
        assert moved["record"]["writer"] != sticky["record"]["writer"]
        self.post({"op": "admit", "key": "main", "writer": sticky["record"]["writer"],
                   "epoch": sticky["record"]["epoch"], "unit": "stale-owner"}, principal=role, ok=False)
        self.upload(30, 1)
        self.start_role("collector4", "adr-flight", ["--config", "/config.json", "--collector", "collector4"],
                        aliases=["collector4"], stage=True)
        self.wait(lambda: len(self.post({"op": "view"})["members"]) == 4)
        self.run(["docker", "stop", "-t", "1", self.names["collector4"]])
        self.wait(lambda: not next(m for m in self.post({"op": "view"})["members"]
                                   if m["writer"]["collector"] == "collector4")["eligible"])
        self.mark("cooperative migration, stale owner rejection, 3-to-4 scale-up and stopped fourth collector")

        self.barrier("before-sql")
        pending = self.executor.submit(self.upload, 90, 1, None, False)
        self.reached("before-sql")
        self.post({"op": "migrate", "key": "main"})
        target = self.post({"op": "view"})["operations"]["main"]["target"]["collector"]
        self.run(["docker", "stop", "-t", "1", self.names[target]])
        self.wait(lambda: self.post({"op": "view"})["operations"]["main"]["target"]["collector"] != target, seconds=35)
        self.release("before-sql")
        completed = pending.result(timeout=35)
        self.wait(lambda: self.final(completed["receipts"][0]["operation"]))
        self.expected["main"] += [90, 91, 92]
        self.wait(lambda: self.assignment()["record"]["lifecycle"] == "Active")
        self.run(["docker", "start", self.names[target]])
        self.wait(lambda: any(m["writer"]["collector"] == target and m["eligible"] for m in self.post({"op": "view"})["members"]))
        self.mark("actual migration target failure releases/replaces durable reservation while source drains")

        # Paused native proposal: collector liveness expires and transfer commits
        # before this publisher is allowed to authorize the old identity.
        self.barrier("before-sql")
        pending = self.executor.submit(self.upload, 100, 1, None, False)
        self.reached("before-sql")
        old = self.assignment()["record"]
        self.run(["docker", "stop", "-t", "1", self.names[old["writer"]["collector"]]])
        self.wait(lambda: self.assignment()["record"]["epoch"] > old["epoch"], seconds=35)
        self.release("before-sql")
        rejected = pending.result(timeout=35)
        assert rejected["terminal"] != "final"
        self.wait(lambda: self.assignment()["record"]["lifecycle"] == "Active")
        assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
        self.run(["docker", "start", self.names[old["writer"]["collector"]]])
        self.mark("actual paused native proposal rejected after authoritative expiry and transfer")

        # Kill the actual publisher after accepted data decision, before any
        # reader-visible log. Recovery must finish that identity without payload replay.
        self.barrier("after-sql")
        pending = self.executor.submit(self.upload, 110, 1, None, False)
        self.reached("after-sql")
        self.run(["docker", "kill", self.names["publisher"]])
        self.release("after-sql")
        uncertain = pending.result(timeout=35)
        operation = uncertain["receipts"][0]["operation"]
        assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
        owner = self.assignment()["record"]
        self.run(["docker", "stop", "-t", "1", self.names[owner["writer"]["collector"]]])
        self.wait(lambda: self.assignment()["record"]["epoch"] > owner["epoch"], seconds=35)
        assert self.assignment()["record"]["lifecycle"] == "Recovering"
        self.run(["docker", "start", self.names["publisher"]])
        self.publisher_port = self.port_of("publisher", 50061)
        self.wait(lambda: self.final(operation))
        self.wait(lambda: self.assignment()["record"]["lifecycle"] == "Active")
        self.run(["docker", "start", self.names[owner["writer"]["collector"]]])
        self.expected["main"] += [110, 111, 112]
        assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
        self.mark("authenticated accepted-before-transfer publisher SIGKILL, successor barrier and identity-only recovery")

        # Lose the final reply after materialization + SQL completion. The
        # gateway has no receipt ledger and must not replay payloads.
        phase = f"after-receipt:{self.scan_runtime()['version'] + 1}"
        self.barrier(phase)
        pending = self.executor.submit(self.upload, 115, 1, None, False)
        self.reached(phase)
        self.run(["docker", "kill", self.names["gateway"]])
        self.release(phase)
        lost = pending.result(timeout=35)
        assert lost["terminal"] != "final"
        self.wait(lambda: self.final(lost["receipts"][0]["operation"]))
        self.expected["main"] += [115, 116, 117]
        if self.arm == "pingora":
            # Router-owned control is deliberately not silently upgraded to HA.
            # Its lease must expire, then its collectors need fresh processes.
            time.sleep(13)
            for name in ("collector1", "collector2", "collector3"):
                self.run(["docker", "restart", "-t", "1", self.names[name]])
        self.run(["docker", "start", self.names["gateway"]])
        self.gateway_port = self.port_of("gateway", 50051)
        if self.arm == "pingora":
            self.control_port = self.port_of("gateway", 50060)
        self.wait(lambda: self.assignment()["record"]["lifecycle"] == "Active")
        assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
        self.mark("gateway death/lost final reply reconciles without replay; router-owned controller restart is conservative")

        # A final SQL update may fail after the ordinary Delta log is visible.
        version = self.scan_runtime()["version"] + 1
        phase = f"before-receipt:{version}"
        self.barrier(phase)
        pending = self.executor.submit(self.upload, 120, 1, None, False)
        self.reached(phase)
        self.run(["docker", "stop", "-t", "1", self.sql_name])
        self.release(phase)
        outcome = pending.result(timeout=35)
        assert outcome["terminal"] != "final", outcome
        self.expected["main"] += [120, 121, 122]
        assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
        self.run(["docker", "start", self.sql_name])
        self.wait_sql()
        self.wait(lambda: self.final(outcome["receipts"][0]["operation"]), seconds=45)
        self.mark("Delta-visible SQL receipt outage returns no final ACK; final status reconciles by identity")
        previous = self.assignment()["record"]
        if self.backend == "etcd":
            self.run(["docker", "stop", "-t", "1", self.names["etcd"]])
            unavailable = self.upload(130, 1, None, False)
            assert unavailable["terminal"] != "final"
            self.post({"op": "delta_receipt", "key": "main",
                       "unit": outcome["receipts"][0]["operation"]}, publisher=True, ok=False)
            assert self.scan_runtime()["rows"] == sorted(self.expected["main"])
            self.mark("real etcd outage fails native admission and authoritative receipt lookup without SQL fallback")
        # A control incarnation is never resurrected after its authoritative
        # expiry. Restart the owned roles conservatively, with fresh processes.
        for role in (self.control_role, "collector1", "collector2", "collector3", "publisher"):
            self.run(["docker", "stop", "-t", "1", self.names[role]])
        time.sleep(13)
        if self.backend == "etcd":
            self.run(["docker", "start", self.names["etcd"]])
            # Lease timers are restored by etcd, not inferred from time the
            # server spent stopped. Let authoritative lease expiry run.
            time.sleep(15)
        self.run(["docker", "start", self.names[self.control_role]])
        self.control_port = self.port_of(self.control_role, 50060)
        if self.arm == "pingora":
            self.gateway_port = self.port_of("gateway", 50051)
        self.wait(lambda: self.post({"op": "view"}))
        for role in ("publisher", "collector1", "collector2", "collector3"):
            self.run(["docker", "start", self.names[role]])
        self.publisher_port = self.port_of("publisher", 50061)
        self.wait(lambda: (a := self.assignment())["record"]["lifecycle"] == "Active"
                  and a["record"]["epoch"] > previous["epoch"])
        self.post({"op": "admit", "key": "main", "writer": previous["writer"],
                   "epoch": previous["epoch"], "unit": "retired-process"},
                  principal=previous["writer"]["collector"], ok=False)
        self.upload(140, 1)
        self.mark("expired controller restart, retired process rejection and fresh native publication")
        for key in self.expected:
            assert self.scan_runtime(key)["rows"] == sorted(self.expected[key])
        (self.root / "expected-rows.json").write_text(json.dumps(self.expected, indent=2))
        self.mark("finite runtime checks passed", arm=self.arm, backend=self.backend, performance=False)

    def finish_runtime(self):
        self.executor.shutdown(wait=True, cancel_futures=True)
        for role, name in self.names.items():
            result = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
            (self.root / "logs" / f"runtime-{role}.log").write_text(result.stdout + result.stderr)
        for name in reversed(self.containers):
            self.run(["docker", "stop", "-t", "1", name])
        super().finish()
        changed = [name for name, digest in self.source_hashes.items()
                   if not (REPO / name).exists() or hashlib.sha256((REPO / name).read_bytes()).hexdigest() != digest]
        (self.root / "runtime-source-drift.json").write_text(json.dumps(changed, indent=2))
        (self.root / "runtime-source-binary-sha256.json").write_text(json.dumps({
            **self.source_hashes,
            **{str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((self.root / "bin").iterdir())}
        }, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--subnet", required=True)
    parser.add_argument("--arm", choices=["adr", "pingora"], required=True)
    parser.add_argument("--backend", choices=["sql", "etcd"], required=True)
    parser.add_argument("--legacy", action="store_true")
    parser.add_argument("--streaming-repro", action="store_true")
    parser.add_argument("--gateway-repair", action="store_true")
    parser.add_argument("--adr-alignment", action="store_true")
    args = parser.parse_args()
    assert not args.legacy or (args.arm == "adr" and args.backend == "etcd")
    runtime = Runtime(args.project, args.subnet, args.arm, args.backend, args.legacy)
    try:
        runtime.setup_runtime()
        if args.adr_alignment:
            runtime.proof_alignment()
        elif args.streaming_repro or args.gateway_repair:
            runtime.proof_gateway_streams(args.streaming_repro)
        else:
            runtime.proof_runtime()
    except Exception as error:
        (runtime.root / "runtime-failure.json").write_text(json.dumps({"error": str(error)}, indent=2))
        print("FAIL", str(error), flush=True)
        raise
    finally:
        runtime.finish_runtime()


if __name__ == "__main__":
    main()
