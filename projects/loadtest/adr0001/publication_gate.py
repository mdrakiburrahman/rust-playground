"""Finite native SQL/Delta publication proof. No benchmark or global cleanup.

Run with the existing target/adr0001/venv (deltalake 1.2.1). All evidence,
secrets, failed attempts, named containers, network and SQL volume are retained.
Only containers created by this invocation are stopped in finally.
"""

import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[3]
BINARY = REPO / "target/debug/publication-gate"
SQL_IMAGE = os.environ.get("ARROWCOL_SQL_IMAGE", "pingora-bench-adrobs20261026final2/sqlserver:local")
RUNTIME_IMAGE = os.environ.get("ARROWCOL_RUNTIME_IMAGE", "pingora-bench-adrobs20261026final2/runtime:local")


class Gate:
    def __init__(self, project, subnet):
        self.project = project
        self.subnet = ipaddress.ip_network(subnet)
        assert project.startswith("publication-gate-") and project.replace("-", "").isalnum()
        assert len(project + "-sql") <= 63, "SQL DNS label exceeds 63 characters"
        assert self.subnet.is_private and self.subnet.prefixlen == 24
        self.root = REPO / "target/publication-gate" / project
        self.root.mkdir(parents=True, exist_ok=False)
        self.containers = []
        self.container_ids = {}
        self.children = []
        self.events = []
        self.passed = []
        self.serial = 0
        self.port = None
        for directory in ("private/tables", "stage", "proposals", "markers", "logs"):
            (self.root / directory).mkdir(parents=True, exist_ok=True)
        (self.root / "private").chmod(0o700)
        (self.root / "stage").chmod(0o777)
        self.password = self.root / "password"
        self.password.write_text(secrets.token_hex(24) + "aA1!\n")
        self.password.chmod(0o600)
        self.a = self.identity("a", 1)
        self.b = self.identity("b", 2)
        self.c = self.identity("c", 3)
        self.d = self.identity("d", 4)
        self.e = self.identity("e", 5)
        self.table = "shared"
        self.visible = []

    def identity(self, name, epoch):
        path = self.root / f"identity-{name}.json"
        path.write_text(json.dumps({
            "controller": f"controller-{name}", "controller_generation": epoch,
            "membership": f"membership-{name}", "process": f"process-{name}",
            "local_generation": epoch, "epoch": epoch,
        }))
        return path

    def run(self, command, ok=True, timeout=90):
        command = [str(c) for c in command]
        command = self.fence_container_command(command)
        started = time.time()
        elapsed_start = time.monotonic()
        result = subprocess.run(command, cwd=REPO, text=True, capture_output=True, timeout=timeout)
        self.serial += 1
        name = f"{self.serial:04d}"
        (self.root / "logs" / f"{name}.stdout").write_text(result.stdout)
        (self.root / "logs" / f"{name}.stderr").write_text(result.stderr)
        self.events.append({
            "id": name, "command": command, "started": started,
            "seconds": time.monotonic() - elapsed_start, "returncode": result.returncode,
        })
        self.save()
        if ok:
            assert result.returncode == 0, (command, result.stderr[-4000:], result.stdout[-2000:])
        else:
            assert result.returncode != 0, ("unexpected success", command, result.stdout)
        return result

    def owned_container(self, name):
        if name in self.container_ids:
            return self.container_ids[name]
        assert name in self.container_ids.values(), ("unowned container", name)
        return name

    def fence_container_command(self, command):
        if command[:1] != ["docker"] or len(command) < 3:
            return command
        command = list(command)
        if command[1] in {"stop", "start", "restart", "kill", "pause", "unpause"}:
            command[-1] = self.owned_container(command[-1])
        elif command[1] == "exec":
            index = 2
            while command[index] in {"-i", "-t", "-it"}:
                index += 1
            command[index] = self.owned_container(command[index])
        return command

    def create_container(self, command):
        name = command[command.index("--name") + 1]
        assert name.startswith(self.project + "-"), ("unowned container name", name)
        assert any(
            command[index:index + 2] == ["--label", f"{label}={self.project}"]
            for index in range(len(command) - 1) for label in ("publication-gate", "delta-runtime")
        ), ("missing container owner label", name)
        assert name not in self.container_ids, ("container already registered", name)
        identity = self.run(command).stdout.strip()
        assert len(identity) == 64 and all(c in "0123456789abcdef" for c in identity), identity
        state = json.loads(self.run(["docker", "inspect", identity]).stdout)[0]
        labels = state["Config"].get("Labels") or {}
        assert state["Id"] == identity and (
            labels.get("publication-gate") == self.project or labels.get("delta-runtime") == self.project
        ), ("container ownership mismatch", name)
        self.container_ids[name] = identity
        self.containers.append(identity)
        return identity

    def create_volume(self, name):
        assert name.startswith(self.project + "-"), ("unowned volume name", name)
        existing = self.run(["docker", "volume", "ls", "-q"]).stdout.split()
        assert name not in existing, ("volume name already owned", name)
        self.run(["docker", "volume", "create", "--label", f"publication-gate={self.project}", name])
        volume = json.loads(self.run(["docker", "volume", "inspect", name]).stdout)[0]
        assert volume["Name"] == name and (volume.get("Labels") or {}).get("publication-gate") == self.project

    def save(self):
        (self.root / "events.json").write_text(json.dumps(self.events, indent=2))
        (self.root / "passed.json").write_text(json.dumps(self.passed, indent=2))

    def mark(self, name, **details):
        self.passed.append({"name": name, "time": time.time(), **details})
        self.save()
        print(f"PASS {name}", flush=True)

    def cli(self, *command):
        return [
            BINARY, "--root", self.root / "private", "--sql-port", self.port,
            "--password-file", self.password, *command,
        ]

    def call(self, *command, ok=True):
        return self.run(self.cli(*command), ok=ok)

    def status(self, table=None):
        return json.loads(self.call("status", table or self.table).stdout)

    def start(self, command, label):
        out = (self.root / "logs" / f"{label}.stdout").open("w")
        err = (self.root / "logs" / f"{label}.stderr").open("w")
        child = subprocess.Popen([str(c) for c in command], cwd=REPO, stdout=out, stderr=err)
        out.close()
        err.close()
        self.children.append(child)
        self.events.append({"label": label, "command": [str(c) for c in command], "pid": child.pid, "started": time.time()})
        self.save()
        return child

    def pause(self, phase, *command):
        label = f"pause-{len(self.children)}"
        marker = self.root / "markers" / label
        child = self.start(self.cli("--pause", phase, "--marker", marker, *command), label)
        until = time.monotonic() + 45
        while not (marker / "paused").exists():
            assert child.poll() is None, ("pause process exited", label)
            assert time.monotonic() < until, ("pause timeout", phase)
            time.sleep(0.05)
        # Real OS stop/resume in addition to the exact native-path barrier.
        os.kill(child.pid, signal.SIGSTOP)
        time.sleep(0.05)
        assert "\nState:\tT" in Path(f"/proc/{child.pid}/status").read_text()
        return child, marker

    def resume(self, paused, success=True):
        child, marker = paused
        (marker / "resume").write_text("resume")
        os.kill(child.pid, signal.SIGCONT)
        code = child.wait(timeout=70)
        assert (code == 0) == success, ("resume result", code)

    def kill(self, child):
        child.kill()
        assert child.wait(timeout=10) == -signal.SIGKILL

    def setup(self):
        names = self.run(["docker", "network", "ls", "-q"]).stdout.split()
        if names:
            networks = json.loads(self.run(["docker", "network", "inspect", *names]).stdout)
            for network in networks:
                assert network["Name"] != self.project, "network name already owned"
                for config in network["IPAM"].get("Config", []) or []:
                    if config.get("Subnet"):
                        other = ipaddress.ip_network(config["Subnet"])
                        assert other.version != 4 or not self.subnet.overlaps(other), ("subnet overlap", other)
        routes = json.loads(self.run(["ip", "-j", "-4", "route"]).stdout)
        for route in routes:
            if route.get("dst", "default") != "default":
                assert not self.subnet.overlaps(ipaddress.ip_network(route["dst"]))
        containers = self.run(["docker", "ps", "-a", "--format", "{{.Names}}"]).stdout.split()
        assert not {self.project + "-sql", self.project + "-collector"}.intersection(containers)
        volumes = self.run(["docker", "volume", "ls", "-q"]).stdout.split()
        assert self.project + "-sql" not in volumes, "volume name already owned"
        self.run(["docker", "network", "create", "--label", f"publication-gate={self.project}",
                  "--subnet", str(self.subnet), self.project])
        self.create_volume(self.project + "-sql")
        self.sql_name = self.project + "-sql"
        self.create_container([
            "docker", "run", "-d", "--name", self.sql_name,
            "--label", f"publication-gate={self.project}", "--network", self.project,
            "--cpus", "2", "--memory", "3g", "--pids-limit", "512",
            "-p", "127.0.0.1::1433",
            "-e", "ACCEPT_EULA=Y", "-e", "MSSQL_PID=Developer",
            "-e", "MSSQL_MEMORY_LIMIT_MB=2048",
            "--mount", f"type=bind,src={self.password},dst=/run/secrets/sqlserver_sa_password,readonly",
            "--mount", f"type=volume,src={self.project}-sql,dst=/var/opt/mssql",
            SQL_IMAGE,
        ])
        self.port = self.run(["docker", "port", self.sql_name, "1433/tcp"]).stdout.strip().split(":")[-1]
        self.wait_sql()
        self.collector = self.project + "-collector"
        self.create_container([
            "docker", "run", "-d", "--name", self.collector,
            "--label", f"publication-gate={self.project}",
            "--network", "none", "--read-only", "--cap-drop=ALL",
            "--security-opt=no-new-privileges", "--user", "21001:21001",
            "--cpus", "1", "--memory", "512m", "--pids-limit", "128",
            "--mount", f"type=bind,src={BINARY},dst=/publication-gate,readonly",
            "--mount", f"type=bind,src={self.root / 'stage'},dst=/stage",
            "--mount", f"type=bind,src={self.root / 'private/tables'},dst=/destination,readonly",
            RUNTIME_IMAGE, "sleep", "1800",
        ])
        self.run(["docker", "exec", self.collector, "/publication-gate", "--help"])
        self.call("bootstrap", self.table, self.a)
        self.mark("isolated SQL and collector ready", loopback_port=self.port, subnet=str(self.subnet))

    def wait_sql(self):
        until = time.monotonic() + 180
        while True:
            check = subprocess.run(
                ["docker", "exec", self.owned_container(self.sql_name), "/usr/local/bin/sqlserver-healthcheck"],
                capture_output=True, text=True,
            )
            if check.returncode == 0:
                break
            assert time.monotonic() < until, check.stderr
            time.sleep(2)

    def stage(self, name, first, evolve=False):
        command = [
            "docker", "exec", self.collector, "/publication-gate", "stage",
            f"/stage/{name}.parquet", "--first", str(first),
        ]
        if evolve:
            command.append("--evolve")
        result = json.loads(self.run(command).stdout)
        return self.root / "stage" / f"{name}.parquet", result["sha256"]

    def prepare(self, name, identity, staged=None, evolve=False, table=None):
        output = self.root / "proposals" / f"{name}.json"
        command = ["prepare", table or self.table, name, identity, output]
        if staged:
            command += ["--stage", staged[0], "--sha256", staged[1]]
        if evolve:
            command.append("--evolve")
        self.call(*command)
        return output

    def scan(self, expected, version, table=None):
        # Separate ordinary delta-rs process, no journal/manifest/catalog adapter.
        code = (
            "import json,sys,gc; from deltalake import DeltaTable; "
            "d=DeltaTable(sys.argv[1]); ds=d.to_pyarrow_dataset(); "
            "t=ds.to_table(use_threads=False,batch_readahead=0,fragment_readahead=1); "
            "print(json.dumps({'version':d.version(),'rows':sorted(t['id'].to_pylist()),"
            "'columns':t.column_names})); del t,ds,d; gc.collect()"
        )
        result = json.loads(self.run([
            sys.executable, "-c", code, self.root / "private/tables" / (table or self.table),
        ]).stdout)
        assert result["version"] == version and result["rows"] == sorted(expected), result
        return result

    def clone(self, source, name, **changes):
        value = json.loads(source.read_text())
        value.update(changes)
        path = self.root / "proposals" / f"{name}.json"
        path.write_text(json.dumps(value))
        return path

    def sql_fixture(self, statement):
        # Administrative fault fixture only. Password never appears in argv/evidence.
        return self.run([
            "docker", "exec", self.sql_name, "bash", "-c",
            'export SQLCMDPASSWORD="$(cat /run/secrets/sqlserver_sa_password)"; '
            'exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -C -b -Q "$1"',
            "fixture", statement,
        ])

    def proof(self):
        self.call("bootstrap", "cold", self.a)
        self.call("transfer", "cold", self.a, self.b)
        self.call("activate", "cold", self.b)
        cold = self.prepare("cold-initialize", self.b, table="cold")
        self.call("accept", cold)
        self.call("transfer", "cold", self.b, self.c)
        self.call("activate", "cold", self.c, ok=False)
        self.call("materialize", "cold")
        self.call("activate", "cold", self.c)
        self.scan([], 0, table="cold")
        self.mark("initialization recovery before and after accepted native CREATE decision")

        initial = self.prepare("initialize", self.a)
        self.call("accept", initial)
        assert self.status()["published"] == -1
        assert not (self.root / "private/tables/shared/_delta_log").exists()
        self.call("materialize", self.table)
        self.scan([], 0)
        base = self.stage("base", 1)
        proposal = self.prepare("base", self.a, base)
        self.scan([], 0)
        self.run([
            "docker", "exec", self.collector, "sh", "-c",
            "printf corrupt > /destination/shared/_delta_log/00000000000000000000.json",
        ], ok=False)
        data_name = json.loads(proposal.read_text())["data"][0]["name"]
        self.run([
            "docker", "exec", self.collector, "sh", "-c",
            f"printf corrupt > /destination/shared/{data_name}",
        ], ok=False)
        self.run([
            "docker", "exec", self.collector, "sh", "-c",
            "chmod u+w /stage/base.parquet; printf changed > /stage/base.parquet",
        ])
        self.call("prepare", self.table, "mutated-staging", self.a,
                  self.root / "proposals/mutated-staging.json",
                  "--stage", base[0], "--sha256", base[1], ok=False)
        self.call("accept", proposal)
        assert self.status()["published"] == 0
        self.scan([], 0)
        self.call("materialize", self.table)
        self.visible = [1, 2, 3]
        self.scan(self.visible, 1)
        self.mark("native Delta, immutable promotion, unratified invisibility and real mount denial")

        late = self.prepare("late-before-sql", self.a, self.stage("late", 10))
        stopped = self.pause("before-sql", "accept", late)
        self.call("transfer", self.table, self.a, self.b)
        self.resume(stopped, success=False)
        assert not json.loads(self.call("resolve", late).stdout)["accepted"]
        self.call("activate", self.table, self.b)
        self.call("accept", late, ok=False)
        current = self.prepare("current", self.b, self.stage("current", 20))
        identity = json.loads(self.b.read_text())
        for field in identity:
            changed = dict(identity)
            changed[field] = changed[field] + 100 if isinstance(changed[field], int) else changed[field] + "-stale"
            invalid = self.clone(current, f"stale-{field}", operation=f"stale-{field}", identity=changed)
            self.call("accept", invalid, ok=False)
        self.run(["docker", "pause", self.sql_name])
        try:
            self.call("accept", current, ok=False)
        finally:
            self.run(["docker", "unpause", self.sql_name])
        assert not json.loads(self.call("resolve", current).stdout)["accepted"]
        self.mark("SQL row-serialized cutover rejects paused, stale tuples and partitioned proposals")

        accepted = self.pause("after-sql", "accept", current)
        assert self.status()["head"] == 2 and self.status()["published"] == 1
        self.call("transfer", self.table, self.b, self.c)
        self.kill(accepted[0])
        self.call("activate", self.table, self.c, ok=False)
        self.call("accept", current)  # Exact recovery is valid, not a new proposal.
        conflicting = self.clone(current, "old-new-operation", operation="old-new-operation")
        self.call("accept", conflicting, ok=False)
        worker = self.pause("before-log:2", "materialize", self.table)
        self.scan(self.visible, 1)
        self.call("materialize", self.table)  # Competing real process.
        self.visible += [20, 21, 22]
        self.scan(self.visible, 2)
        self.resume(worker)
        self.call("activate", self.table, self.c)
        assert self.status()["active"]
        self.mark("accepted-before-cutover recovery, successor wait, duplicate exact-version workers")

        receipt = self.prepare("receipt-outage", self.c, self.stage("receipt-outage", 30))
        self.call("accept", receipt)
        stopped = self.pause("after-log:3", "materialize", self.table)
        self.visible += [30, 31, 32]
        self.scan(self.visible, 3)
        assert self.status()["published"] == 2
        self.run(["docker", "pause", self.sql_name])
        try:
            self.resume(stopped, success=False)
        finally:
            self.run(["docker", "unpause", self.sql_name])
        assert self.status()["published"] == 2
        self.call("materialize", self.table)
        assert self.status()["published"] == 3 and self.status()["receipts"] == 4
        self.scan(self.visible, 3)
        self.mark("Delta-visible receipt-write outage reconciles without data duplication")

        lost = self.prepare("lost-sql-reply", self.c, self.stage("lost-sql-reply", 40))
        child = self.start(self.cli("accept", lost, "--lose-reply"), "lost-sql-reply")
        until = time.monotonic() + 20
        while self.status()["head"] != 4:
            assert time.monotonic() < until and child.poll() is None
            time.sleep(0.1)
        assert child.poll() is None
        self.kill(child)
        assert json.loads(self.call("resolve", lost).stdout)["accepted"]
        collision = self.clone(lost, "collision", actions=json.loads(lost.read_text())["actions"] + '{"commitInfo":{}}\n')
        self.call("resolve", collision, ok=False)
        self.call("accept", collision, ok=False)
        self.call("transfer", self.table, self.c, self.d)
        self.call("activate", self.table, self.d, ok=False)
        self.call("materialize", self.table)
        self.visible += [40, 41, 42]
        self.scan(self.visible, 4)
        self.call("activate", self.table, self.d)
        self.mark("durable SQL COMMIT with lost client reply resolved by exact operation identity")

        crash = self.prepare("partial-private-crash", self.d, self.stage("partial-private-crash", 50))
        self.call("accept", crash)
        stopped = self.pause("during-log:5", "materialize", self.table)
        self.scan(self.visible, 4)
        assert not (self.root / "private/tables/shared/_delta_log/00000000000000000005.json").exists()
        self.kill(stopped[0])
        assert list((self.root / "private/atomic").glob("*.candidate"))
        self.call("materialize", self.table)
        self.visible += [50, 51, 52]
        self.scan(self.visible, 5)
        self.mark("SIGKILL during half-written private log leaves ordinary reader on complete prefix")

        storage = self.prepare("storage-fault", self.d, self.stage("storage-fault", 60))
        self.call("accept", storage)
        directory = self.root / "private/tables/shared/_delta_log"
        directory.chmod(0o555)
        try:
            self.call("materialize", self.table, ok=False)
            self.scan(self.visible, 5)
            assert self.status()["published"] == 5
        finally:
            directory.chmod(0o755)
        self.call("materialize", self.table)
        self.visible += [60, 61, 62]
        self.scan(self.visible, 6)
        self.mark("real local storage permission failure has no new version or final receipt")

        evolution = self.prepare("nullable-schema", self.d, evolve=True)
        self.call("accept", evolution)
        self.scan(self.visible, 6)
        self.call("materialize", self.table)
        assert "extra" in self.scan(self.visible, 7)["columns"]
        evolved = self.prepare("evolved-data", self.d, self.stage("evolved-data", 70, True), evolve=True)
        rival = self.prepare("same-predecessor", self.d, self.stage("same-predecessor", 80, True), evolve=True)
        one = self.start(self.cli("accept", evolved), "concurrent-accept-one")
        two = self.start(self.cli("accept", rival), "concurrent-accept-two")
        results = [one.wait(timeout=30), two.wait(timeout=30)]
        assert sum(code == 0 for code in results) == 1, results
        self.call("materialize", self.table)
        self.visible += [70, 71, 72] if results[0] == 0 else [80, 81, 82]
        self.scan(self.visible, 8)
        self.mark("native nullable schema evolution and concurrent expected-version acceptance")

        restart = self.prepare("sql-restart", self.d, self.stage("sql-restart", 100, True), evolve=True)
        self.call("accept", restart)
        self.run(["docker", "pause", self.sql_name])
        try:
            self.call("materialize", self.table, ok=False)
            self.scan(self.visible, 8)
        finally:
            self.run(["docker", "unpause", self.sql_name])
        stopped = self.pause("after-log:9", "materialize", self.table)
        self.kill(stopped[0])
        self.visible += [100, 101, 102]
        self.scan(self.visible, 9)
        assert self.status()["published"] == 8
        self.run(["docker", "restart", "-t", "10", self.sql_name], timeout=50)
        self.wait_sql()
        self.port = self.run(["docker", "port", self.sql_name, "1433/tcp"]).stdout.strip().split(":")[-1]
        assert json.loads(self.call("resolve", restart).stdout)["accepted"]
        self.call("transfer", self.table, self.d, self.e)
        self.call("activate", self.table, self.e, ok=False)
        self.call("materialize", self.table)
        self.call("activate", self.table, self.e)
        self.scan(self.visible, 9)
        self.mark("post-log SIGKILL, SQL restart durability, authority outage and receipt-gated successor")

        ack = self.prepare("lost-final-reply", self.e, self.stage("lost-final-reply", 110, True), evolve=True)
        self.call("accept", ack)
        stopped = self.pause("after-receipt:10", "materialize", self.table)
        self.kill(stopped[0])
        assert self.status()["published"] == 10 and self.status()["receipts"] == 11
        self.visible += [110, 111, 112]
        self.scan(self.visible, 10)
        self.call("materialize", self.table)
        self.scan(self.visible, 10)
        self.mark("lost final reply resolves materialized receipt without replaying data")

        self.call("bootstrap", "corrupt", self.a)
        corrupt = self.prepare("corrupt-initialize", self.a, table="corrupt")
        self.call("accept", corrupt)
        corrupt_log = self.root / "private/tables/corrupt/_delta_log"
        corrupt_log.mkdir()
        (corrupt_log / "00000000000000000000.json").write_text("{")
        self.call("materialize", "corrupt", ok=False)
        assert self.status("corrupt")["published"] == -1
        assert (corrupt_log / "00000000000000000000.json").read_text() == "{"
        self.mark("preexisting incomplete/conflicting final log fails closed, never overwritten")

        expired = self.prepare("expired-owner", self.e, self.stage("expired-owner", 90, True), evolve=True)
        self.sql_fixture("UPDATE publication_gate.owners SET expires=DATEADD(second,-1,SYSUTCDATETIME()) WHERE table_id='shared'")
        self.call("accept", expired, ok=False)
        assert not json.loads(self.call("resolve", expired).stdout)["accepted"]
        self.scan(self.visible, 10)
        self.mark("database-clock expiry rejects exact but expired incarnation")
        (self.root / "final-status.json").write_text(json.dumps(self.status(), indent=2))
        (self.root / "expected-rows.json").write_text(json.dumps(sorted(self.visible)))

    def finish(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=10)
        for name in self.containers:
            state = json.loads(subprocess.check_output(["docker", "inspect", name]))[0]["State"]
            if state.get("Paused"):
                self.run(["docker", "unpause", name])
            self.run(["docker", "stop", "-t", "15", name], timeout=40)
        if self.containers:
            inventory = self.run(["docker", "inspect", *self.containers]).stdout
            (self.root / "stopped-containers.json").write_text(inventory)
            assert all(not c["State"]["Running"] for c in json.loads(inventory))
        files = [
            BINARY, REPO / "Cargo.lock",
            REPO / "crates/arrowcol-authority/Cargo.toml",
            REPO / "crates/arrowcol-authority/src/lib.rs",
            REPO / "crates/arrowcol-authority/src/sql_publication.rs",
            REPO / "crates/arrowcol-authority/src/sql_publication.sql",
            REPO / "crates/arrowcol-authority/src/bin/publication-gate.rs",
            REPO / "crates/arrow-deltalake/src/delta.rs",
            Path(__file__),
            REPO / "projects/loadtest/adr0001/PUBLICATION_GATE.md",
        ]
        for crate in ("arrowcol-authority", "arrowcol-sql-server", "arrow-deltalake", "arrowcol-protocol"):
            files += list((REPO / "crates" / crate / "src").rglob("*.rs"))
            files.append(REPO / "crates" / crate / "Cargo.toml")
        (self.root / "source-binary-sha256.json").write_text(json.dumps({
            str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(files))
        }, indent=2))
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--subnet", required=True)
    args = parser.parse_args()
    gate = Gate(args.project, args.subnet)
    try:
        gate.setup()
        gate.proof()
        gate.mark("BOUNDED PUBLICATION GATE PASSED", full_benchmark=False, routing_integrations=False)
    finally:
        gate.finish()


if __name__ == "__main__":
    main()
