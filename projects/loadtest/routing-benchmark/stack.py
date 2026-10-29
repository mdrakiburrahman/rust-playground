"""Task-owned Compose lifecycle; no daemon management or global cleanup."""

import argparse
import copy
import ipaddress
import json
import os
import secrets
import subprocess
import time
import uuid
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[3]
BASE = REPO / "projects/loadtest"


def write(path, value, secret=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)
    if secret:
        path.chmod(0o600)


def prepare(project, subnet, policy, resume=False):
    if not project.startswith("pingora-bench-") or not project.replace("-", "").isalnum():
        raise ValueError("project must be a safe pingora-bench-* identifier")
    network = ipaddress.ip_network(subnet)
    if network.version != 4 or network.prefixlen != 24 or not network.is_private:
        raise ValueError("use a private IPv4 /24")
    root = REPO / "target/routing-benchmark" / project
    if root.exists() and not resume:
        raise ValueError("task directory exists; resume it instead of overwriting state")
    if resume:
        previous, _ = context(root)
        if previous["policy"] != policy or previous["subnet"] != subnet:
            raise ValueError("cannot change persisted task identity or policy")
    root.mkdir(parents=True, exist_ok=True)
    write(root.parent / ".dockerignore", "*\n!bin/\n!bin/**\n!sdk-toolchain/\n!sdk-toolchain/**\n")
    base = yaml.safe_load((BASE / "compose.yaml").read_text())
    chosen = ("sqlserver", "database-bootstrap", "arrowcol-migrate", "hive-schema", "hive-metastore")
    services = {name: copy.deepcopy(base["services"][name]) for name in chosen}
    secret_files = {}
    for name in ("sqlserver_sa_password", "arrowcol_app_password", "arrowcol_migrator_password", "hive_metastore_password",
                 "edge_hmac", "internal_hmac", "operator_token", "router2_token", *(f"collector{i}_token" for i in range(1, 5))):
        path = root / "secrets" / name
        if not path.exists():
            if resume:
                raise ValueError("existing task secret missing; refusing replacement")
            write(path, secrets.token_hex(32) + "aA1!\n", secret=True)
        secret_files[name] = {"file": str(path)}
    credentials = [
        {"role": "operator", "instance_key": None, "token_file": "/run/secrets/operator_token"},
        {"role": "data_router", "instance_key": None, "token_file": "/run/secrets/router2_token"},
    ] + [{"role": "collector", "instance_key": f"routing-benchmark-direct-{i}", "token_file": f"/run/secrets/collector{i}_token"} for i in range(1, 5)]
    write(root / "controller-state" / "credentials.json", json.dumps(credentials))
    operator_header = root / "secrets/operator_header"
    write(operator_header, "Authorization: Bearer " + (root / "secrets/operator_token").read_text().strip() + "\n", secret=True)
    secret_files["operator_header"] = {"file": str(operator_header)}
    state = {"project": project, "subnet": subnet, "policy": policy, "root": str(root), "created_at": time.time()}
    if not resume:
        write(root / "ownership.json", json.dumps(state, indent=2))
    for name, service in services.items():
        service.pop("profiles", None)
        service.pop("networks", None)
        if "build" in service:
            service["build"]["context"] = str((BASE / service["build"]["context"]).resolve())
        service["labels"] = {"io.arrowcol.routing-benchmark": project}
    services["sqlserver"]["environment"]["MSSQL_MEMORY_LIMIT_MB"] = "2048"
    services["sqlserver"]["mem_limit"] = "3g"
    services["sqlserver"]["cpus"] = 2
    services["sqlserver"]["ports"] = ["127.0.0.1::1433"]
    services["sqlserver"]["image"] = f"{project}/sqlserver:local"
    services["database-bootstrap"]["image"] = services["sqlserver"]["image"]
    services["arrowcol-migrate"]["image"] = services["sqlserver"]["image"]
    services["hive-schema"]["image"] = f"{project}/hive:local"
    services["hive-metastore"]["image"] = services["hive-schema"]["image"]
    services["hive-metastore"]["mem_limit"] = "1g"
    services["hive-metastore"]["cpus"] = 1
    services["hive-metastore"]["environment"]["JAVA_TOOL_OPTIONS"] = "-Xms128m -Xmx512m"
    services["hive-metastore"].pop("ports", None)
    image = f"{project}/runtime:local"
    for i in range(1, 5):
        direct = copy.deepcopy(base["services"]["direct-1"])
        direct.pop("profiles", None)
        direct["image"] = image
        direct["entrypoint"] = ["/usr/local/bin/arrowcol-rust"]
        direct["user"] = f"{os.getuid()}:{os.getgid()}"
        direct["cpus"], direct["mem_limit"] = 1, "1g"
        direct["environment"] = {
            "TOKIO_WORKER_THREADS": "2", "ARROWCOL_CONTROLLER_ENDPOINT": "http://router:50060",
            "ARROWCOL_CONTROL_TOKEN_FILE": f"/run/secrets/collector{i}_token",
        }
        direct["secrets"].append(f"collector{i}_token")
        direct["depends_on"] = {
            "hive-metastore": {"condition": "service_healthy"},
            "arrowcol-migrate": {"condition": "service_completed_successfully"},
            "router": {"condition": "service_healthy"},
        }
        direct["networks"] = {"default": {"ipv4_address": str(network[20 + i])}}
        direct["volumes"] = [
            {"type": "bind", "source": str(root / "direct"), "target": "/delta/direct"},
            {"type": "bind", "source": str(root / f"direct-state-{i}"), "target": "/state"},
        ]
        direct["labels"] = {"io.arrowcol.routing-benchmark": project}
        direct["healthcheck"] = {"test": ["CMD", "curl", "-fsS", "--max-time", "2", "http://127.0.0.1:8081/readyz"], "interval": "5s", "retries": 30, "start_period": "30s"}
        if i == 4:
            direct["profiles"] = ["scale"]
        services[f"direct-{i}"] = direct
        template = (BASE / "config/direct.yaml.tmpl").read_text().replace("__ADVERTISED_ENDPOINT__", f"{network[20+i]}:50051").replace("__INSTANCE_KEY__", f"routing-benchmark-direct-{i}").replace("__GENERATION__", "1")
        config = yaml.safe_load(template)
        config["buffer"]["global_capacity"].update(bytes=134217728, rows=4000000, batches=2048)
        config["buffer"]["per_table_capacity"].update(bytes=33554432, rows=1000000, batches=512)
        config["buffer"]["max_arrow_memory_per_flush"] = 33554432
        config["routing"].update(registration_heartbeat_ms=1000, max_snapshot_age_ms=3000, rss_limit_bytes=1073741824)
        config["shutdown"].update(stream_drain_timeout_ms=30000, publication_drain_timeout_ms=60000)
        write(root / f"direct-state-{i}/direct.yaml", yaml.safe_dump(config))
    (root / "direct").mkdir(exist_ok=True)
    router_config = yaml.safe_load((BASE / "config/router.yaml").read_text())
    router_config["server"]["threads"] = 2
    router_config["server"]["grace_period_seconds"] = 65
    router_config["server"]["graceful_shutdown_timeout_seconds"] = 60
    router_config["routing"].update(refresh_interval_ms=1000, max_snapshot_age_ms=3000, lease_ttl_ms=30000, readiness_max_staleness_ms=3000)
    router_config["telemetry"]["log_filter"] = "info,arrowcol_router_rust=debug,arrowcol_routing=debug"
    for i, name in enumerate(("router", "router2")):
        config_root = root / ("controller-state" if i == 0 else "router2-state")
        write(config_root / "config.yaml", yaml.safe_dump(router_config))
        router = copy.deepcopy(base["services"]["router"])
        router.pop("profiles", None)
        router.update(image=image, entrypoint=["/usr/local/bin/arrowcol-router-rust"], user=f"{os.getuid()}:{os.getgid()}", cpus=1, mem_limit="1g")
        router["build"] = {"context": str(root.parent), "dockerfile": str(BASE / "routing-benchmark/Dockerfile")}
        router["depends_on"] = {"arrowcol-migrate": {"condition": "service_completed_successfully"}}
        router["ports"] = ["127.0.0.1::50050"]
        router["networks"] = {"default": {"ipv4_address": str(network[10 + i])}}
        router["volumes"] = [{"type": "bind", "source": str(config_root), "target": "/state"}]
        router["secrets"].append("operator_header")
        router["environment"] = {"ARROWCOL_ROUTING_MODE": "controller" if i == 0 else "data_only"}
        if i == 0:
            router["environment"].update(
                ARROWCOL_ROUTING_DIRECTORY="/state/routing", ARROWCOL_CONTROL_LISTEN="0.0.0.0:50060",
                ARROWCOL_CONTROL_CREDENTIALS_FILE="/state/credentials.json", ARROWCOL_PLACEMENT_POLICY=policy,
            )
            router["secrets"].extend(name for name in secret_files if name.endswith("_token"))
            # Startup health is private-authority liveness, not ingestion
            # readiness, so collectors never wait on their own registration.
            router["healthcheck"] = {
                "test": ["CMD-SHELL", """curl -fsS --max-time 2 --header @/run/secrets/operator_header --header 'Content-Type: application/json' --data '{"operation":"view"}' http://127.0.0.1:50060/v1/routing | grep -q '"result":"view"'"""],
                "interval": "2s", "timeout": "3s", "retries": 30, "start_period": "5s",
            }
        else:
            router["profiles"] = ["scale"]
            router["secrets"].append("router2_token")
            router["environment"].update(ARROWCOL_CONTROLLER_ENDPOINT="http://router:50060", ARROWCOL_CONTROL_TOKEN_FILE="/run/secrets/router2_token")
        router["labels"] = {"io.arrowcol.routing-benchmark": project}
        services[name] = router
    (root / "prepared").mkdir(exist_ok=True)
    services["prepare"] = {
        "image": image, "profiles": ["prepare"], "pull_policy": "never",
        "user": f"{os.getuid()}:{os.getgid()}", "cpus": 1, "mem_limit": "1g",
        "working_dir": "/work", "entrypoint": ["/usr/local/bin/arrowcol-loadgen"],
        "command": [
            "prepare", "--router-endpoint", "http://router:50050",
            "--catalog-endpoint", "http://direct-1:50051", "--receipt-endpoint", "http://router:50050",
            "--edge-secret-file", "/run/secrets/edge-hmac", "--internal-secret-file", "/run/secrets/internal-hmac",
            "--sql-host", "sqlserver", "--sql-password-file", "/run/secrets/arrowcol-app-password",
            "--scratch-root", "target/sdk", "--results", "/work/results", "--sdk-generator", "/usr/local/bin/arrowcol-sdk-gen",
            "--max-attempts", "1",
        ],
        "secrets": copy.deepcopy(services["router"]["secrets"][:3]),
        "volumes": [{"type": "bind", "source": str(root / "prepared"), "target": "/work"}],
        "labels": {"io.arrowcol.routing-benchmark": project},
    }
    # Fixed scope names are Compose-project qualified. Never use external/global volumes.
    compose = {
        "name": project, "services": services, "secrets": secret_files,
        "volumes": {"sqlserver-data": {}, "hive-warehouse": {}},
        "networks": {"default": {"ipam": {"config": [{"subnet": subnet}]}}},
    }
    write(root / "compose.yaml", yaml.safe_dump(compose, sort_keys=False))
    # Base secret paths are replaced; environment interpolation remains only for harmless stop timeouts.
    print(root)
    return root


def context(root):
    root = Path(root).resolve()
    state = json.loads((root / "ownership.json").read_text())
    if root != REPO / "target/routing-benchmark" / state["project"]:
        raise ValueError("ownership marker does not match task directory")
    return state, ["docker", "--context", "default", "compose", "--project-name", state["project"], "--file", str(root / "compose.yaml")]


def control(root, operation, request=None):
    _, command = context(root)
    payload = {"operation": operation}
    if request is not None:
        payload["request"] = request
    result = subprocess.run(command + [
        "exec", "-T", "router", "curl", "-fsS", "--max-time", "5",
        "--header", "@/run/secrets/operator_header", "--header", "Content-Type: application/json",
        "--data-binary", "@-", "http://127.0.0.1:50060/v1/routing",
    ], input=json.dumps(payload), text=True, check=True, capture_output=True)
    response = json.loads(result.stdout)
    if response["result"] == "error":
        raise RuntimeError(response["value"])
    return response


def move(root, key, scale_in=False, timeout=90, record=None):
    started = time.monotonic()
    if record is None:
        record = control(root, "migrate", [key, "finite benchmark controlled move", scale_in])
    elif record.get("result") != "migration" or record["value"]["key"] != key:
        raise ValueError("migration observation requires the exact acknowledged table operation")
    while time.monotonic() - started < timeout:
        view = control(root, "view")["value"]
        lease = next(l for l in view["leases"] if l["key"] == key)
        if lease["holder_instance_id"] == record["value"]["target"] and lease["state"] == "Active":
            return {"migration": record["value"], "activation_seconds": time.monotonic() - started, "lease": lease}
        time.sleep(1)
    raise TimeoutError("bounded migration blocked; do not remove source or force ownership")


def scale_in(root):
    state, command = context(root)
    view = control(root, "view")["value"]
    source = next(c for c in view["collectors"] if c["registration"]["instance_key"] == "routing-benchmark-direct-4")
    instance = source["registration"]["instance_id"]
    control(root, "drain_collector", instance)
    evidence = []
    for lease in view["leases"]:
        if lease["holder_instance_id"] == instance:
            evidence.append(move(root, lease["key"], scale_in=True))
            time.sleep(5)
    view = control(root, "view")["value"]
    collector = next(c for c in view["collectors"] if c["registration"]["instance_id"] == instance)
    pressure = collector.get("pressure")
    if (any(l["holder_instance_id"] == instance for l in view["leases"]) or pressure is None
            or abs(time.time_ns() // 1_000_000 - pressure["observed_at_unix_ms"]) > 3000):
        raise RuntimeError("scale-in blocked: ownership or pressure unresolved")
    for name in ("active_streams", "buffered_bytes", "buffered_rows", "buffered_batches", "pending_commits"):
        if pressure[name]["current"] != 0:
            raise RuntimeError("scale-in blocked by admitted/buffered work")
    subprocess.run(command + ["stop", "--timeout", "95", "direct-4"], check=True)
    stopped = subprocess.check_output(command + ["ps", "-a", "--format", "json", "direct-4"], text=True)
    record = json.loads(stopped)
    if record.get("State") != "exited" or record.get("ExitCode") != 0:
        raise RuntimeError("collector did not complete graceful shutdown; preserve its state")
    return evidence


def rust_smoke(root):
    from native import visible_markers

    _, command = context(root)
    root = Path(root).resolve()
    compose = yaml.safe_load((root / "compose.yaml").read_text())
    arguments = list(compose["services"]["prepare"]["command"])
    arguments[0] = "run"
    result_name = "rust-native-smoke-" + uuid.uuid4().hex[:8]
    arguments[arguments.index("--results") + 1] = "/work/" + result_name
    table = json.loads((root / "prepared/results/prepared-tables.json").read_text())[0]["identity"]
    before_rows = len(visible_markers(root / "direct", table))
    arguments.extend([
        "--scenario", "direct", "--table", table, "--warmup", "0s", "--measure", "1s",
        "--capture-settle", "0s", "--drain-timeout", "30s", "--request-timeout", "5s",
        "--lanes", "1", "--batch-rows", "8",
    ])
    subprocess.run(command + [
        "run", "--rm", "--no-deps", "--volume", str(root / "direct") + ":/delta/direct:ro",
        "prepare", *arguments,
    ], check=True, timeout=180)
    result_root = root / "prepared" / result_name
    sends = [json.loads(line) for line in (result_root / "sent-batches.jsonl").read_text().splitlines()]
    receipts = [json.loads(line) for line in (result_root / "receipts.jsonl").read_text().splitlines()]
    after_rows = len(visible_markers(root / "direct", table))
    expected = sum(send["rows"] for send in sends)
    reconciled = (
        bool(sends) and len(sends) == len(receipts)
        and all(send["attempt"] == 1 and send["outcome"] == "durable_receipt" for send in sends)
        and {send["receipt_id"] for send in sends} == {receipt["receipt_id"] for receipt in receipts}
        and all(receipt["state"] == "published" and receipt["purge_allowed"] for receipt in receipts)
        and after_rows - before_rows == expected
    )
    (result_root / "independent-output.json").write_text(json.dumps({
        "table": table, "before_rows": before_rows, "after_rows": after_rows,
        "receipt_confirmed_rows": expected, "reconciled": reconciled,
        "requires_other_writers_quiescent": True, "exactly_once_claim": False,
        "legacy_sql_routing_evidence_is_not_disk_ownership_evidence": True,
    }, indent=2))
    if not reconciled:
        raise RuntimeError("unchanged Rust client receipts/output did not reconcile")
    print(json.dumps({"evidence": str(result_root), "reconciled": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("prepare", "configure", "up", "schemas", "rust-smoke", "stop", "status", "view", "add-collector", "remove-collector", "add-router", "remove-router", "migrate"))
    parser.add_argument("--root")
    parser.add_argument("--project", default="pingora-bench-" + uuid.uuid4().hex[:10])
    parser.add_argument("--subnet", default="10.198.37.0/24")
    parser.add_argument("--policy", choices=("least_pressure", "weighted_rendezvous"), default="least_pressure")
    parser.add_argument("--table")
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.project, args.subnet, args.policy)
    else:
        state, command = context(args.root)
        if args.action == "configure":
            prepare(state["project"], state["subnet"], state["policy"], resume=True)
        elif args.action == "view":
            print(json.dumps(control(args.root, "view"), indent=2))
        elif args.action == "migrate":
            catalog, schema, table = args.table.split(".")
            print(json.dumps(move(args.root, {"implementation_pool": "direct", "table": {"catalog": catalog, "schema": schema, "table": table}}), indent=2))
        elif args.action == "remove-collector":
            print(json.dumps(scale_in(args.root), indent=2))
        elif args.action == "rust-smoke":
            rust_smoke(args.root)
        else:
            commands = {
                "up": ["up", "-d", "--build", "--wait", "--wait-timeout", "120", "router", "direct-1", "direct-2", "direct-3"],
                "schemas": ["run", "--rm", "--no-deps", "prepare"],
                "stop": ["--profile", "*", "stop", "--timeout", "95"],
                "status": ["ps"],
                "add-collector": ["up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "direct-4"],
                "add-router": ["up", "-d", "--no-deps", "--wait", "--wait-timeout", "60", "router2"],
                "remove-router": ["stop", "--timeout", "95", "router2"],
            }
            subprocess.run(command + commands[args.action], check=True)
