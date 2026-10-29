"""Task-owned authenticated three-member etcd and native Rust Flight stack.

No daemon management, global cleanup, source secrets, or volume deletion.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time

import yaml

REPO = Path(__file__).resolve().parents[3]
ETCD_IMAGE = "registry.k8s.io/etcd:3.6.4-0"
RUNTIME_IMAGE = "pingora-bench-least-final/runtime:local"


def save(path, value, private=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value)
    if private:
        path.chmod(0o600)


def compose(root):
    root = Path(root).resolve()
    state = json.loads((root / "ownership.json").read_text())
    if root != REPO / "target/adr0001" / state["project"] or not state["project"].startswith("adr0001-"):
        raise ValueError("task ownership mismatch")
    return ["docker", "--context", "default", "compose", "-p", state["project"], "-f", str(root / "compose.yaml")]


def prepare(project, catalog):
    if not project.startswith("adr0001-") or not project.replace("-", "").isalnum():
        raise ValueError("unsafe task project")
    root = REPO / "target/adr0001" / project
    root.mkdir(parents=True, exist_ok=False)
    save(root / "ownership.json", json.dumps({"project": project, "created_ns": time.time_ns()}))
    storage = root / "objects"
    storage.mkdir()
    (root / "bin").mkdir()
    for binary in ("adr-controller","adr-flight"):
        shutil.copy2(REPO / "target/debug" / binary,root / "bin" / binary)
    config = {
        "tenant": "benchmark-edge-principal", "catalog_id": "benchmark-catalog-v1",
        "tables": catalog, "storage": "/objects", "controller": "http://controller:50060",
        "operator_token": secrets.token_hex(32), "gateway_token": secrets.token_hex(32),
        "edge_secret": secrets.token_hex(32), "internal_secret": secrets.token_hex(32),
        "collectors": {f"collector{i}": secrets.token_hex(32) for i in range(1, 5)},
        "etcd": [f"http://etcd{i}:2379" for i in range(1, 4)],
        "etcd_user": "controller", "etcd_password": secrets.token_hex(32),
    }
    save(root / "secrets/controller.json", json.dumps(config), True)
    for role in ("gateway", *(f"collector{i}" for i in range(1, 5))):
        scoped = dict(config, etcd=[], etcd_user="", etcd_password="", operator_token="")
        if role == "gateway":
            scoped["collectors"] = {}
        else:
            scoped["gateway_token"] = ""
            scoped["collectors"] = {role: config["collectors"][role]}
        save(root / f"secrets/{role}.json", json.dumps(scoped), True)
    root_password = secrets.token_hex(32)
    save(root / "secrets/etcd-root", root_password, True)
    members = ",".join(f"etcd{i}=http://etcd{i}:2380" for i in range(1, 4))
    services = {}
    for i in range(1, 4):
        name = f"etcd{i}"
        services[name] = {
            "image": ETCD_IMAGE, "entrypoint": ["/usr/local/bin/etcd"],
            "command": [f"--name={name}", "--data-dir=/etcd-data",
                        "--listen-client-urls=http://0.0.0.0:2379", f"--advertise-client-urls=http://{name}:2379",
                        "--listen-peer-urls=http://0.0.0.0:2380", f"--initial-advertise-peer-urls=http://{name}:2380",
                        f"--initial-cluster={members}", f"--initial-cluster-token={project}", "--initial-cluster-state=new",
                        "--auto-compaction-retention=1h"],
            "volumes": [f"etcd{i}-data:/etcd-data"], "networks": ["coordination"],
            "cpus": 0.5, "mem_limit": "256m",
        }
    for role in ("controller", "gateway1", "gateway2", *(f"collector{i}" for i in range(1, 5))):
        is_controller = role == "controller"
        config_role = "gateway" if role.startswith("gateway") else role
        binary = "adr-controller" if is_controller else "adr-flight"
        command = ["/native/" + binary, "--config", "/config.json"]
        if role.startswith("collector"):
            command += ["--collector", role]
        services[role] = {
            "image": RUNTIME_IMAGE, "entrypoint": command,
            "user": f"{os.getuid()}:{os.getgid()}",
            "volumes": [f"{root / 'bin'}:/native:ro",
                        f"{root / 'secrets' / (config_role + '.json')}:/config.json:ro",
                        f"{storage}:/objects"],
            "networks": ["coordination"] if is_controller else ["coordination", "data"],
            "cpus": 1, "mem_limit": "1g", "stop_grace_period": "5s",
        }
        if role.startswith("gateway"):
            services[role]["ports"] = ["127.0.0.1::50051"]
            services[role]["networks"].append("ingress")
        if role in ("gateway2", "collector4"):
            services[role]["profiles"] = ["scale"]
    spec = {"name": project, "services": services,
            "volumes": {f"etcd{i}-data": {} for i in range(1, 4)},
            "networks": {"coordination": {"internal": True}, "data": {"internal": True}, "ingress": {}}}
    for service in services.values():
        service["labels"] = {"io.arrowcol.adr0001": project}
    save(root / "compose.yaml", yaml.safe_dump(spec, sort_keys=False))
    return root


def etcdctl(root, *args, authenticate=False):
    root = Path(root)
    environment = []
    if authenticate:
        # Credentials are supplied through stdin/environment of this exact task-owned process.
        password = (root / "secrets/etcd-root").read_text()
        environment = ["-e", f"ETCDCTL_USER=root:{password}"]
    return subprocess.run(compose(root) + ["exec", "-T", *environment, "etcd1", "/usr/local/bin/etcdctl",
        "--endpoints=http://etcd1:2379", *args], check=True, text=True, capture_output=True).stdout


def start(root):
    root = Path(root)
    command = compose(root)
    subprocess.run(command + ["up", "-d", "etcd1", "etcd2", "etcd3"], check=True)
    for _ in range(60):
        try:
            etcdctl(root, "endpoint", "health")
            break
        except subprocess.CalledProcessError:
            time.sleep(0.5)
    else:
        raise RuntimeError("bounded etcd readiness expired")
    config = json.loads((root / "secrets/controller.json").read_text())
    # The only bootstrap interval is private, before application processes start.
    for args in (
        ("user", "add", "root", "--new-user-password=" + (root / "secrets/etcd-root").read_text()),
        ("user", "grant-role", "root", "root"),
        ("user", "add", "controller", "--new-user-password=" + config["etcd_password"]),
        ("role", "add", "controller"),
        ("role", "grant-permission", "controller", "readwrite", "/adr/", "--prefix"),
        ("user", "grant-role", "controller", "controller"),
        ("auth", "enable"),
    ):
        etcdctl(root, *args)
    subprocess.run(command + ["up", "-d", "controller", "collector1", "collector2", "gateway1"], check=True)


def control(root, operation, role="operator"):
    root = Path(root)
    config = json.loads((root / "secrets/controller.json").read_text())
    token = config.get(role + "_token", config["collectors"].get(role))
    # No public control port. curl is inside this exact task-owned controller container.
    result = subprocess.run(compose(root) + [
        "exec", "-T", "controller", "curl", "-sS", "--max-time", "8", "-w", "\n%{http_code}",
        "-H", "Authorization: Bearer " + token, "-H", "Content-Type: application/json",
        "--data-binary", "@-", "http://127.0.0.1:50060/v1/authority",
    ], input=json.dumps(operation), text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError("private authority transport failed: " + result.stderr[:512])
    body, code = result.stdout.rsplit("\n", 1)
    value = json.loads(body)
    if code != "200":
        raise RuntimeError(json.dumps({"http": code, "value": value}))
    return value


def wait(root, predicate, seconds=25):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = control(root, {"op": "view"})
            if predicate(last):
                return last
        except (RuntimeError, subprocess.CalledProcessError):
            pass
        time.sleep(0.2)
    raise TimeoutError(f"state predicate timed out: {last}")


def endpoint(root, gateway="gateway1"):
    address = subprocess.check_output(compose(root) + ["port", gateway, "50051"], text=True).strip()
    if not address.startswith("127.0.0.1:"):
        raise ValueError("non-loopback ingress")
    return "grpc://" + address


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["prepare", "start", "stop", "view"])
    parser.add_argument("--project")
    parser.add_argument("--catalog")
    parser.add_argument("--root")
    args = parser.parse_args()
    if args.action == "prepare":
        print(prepare(args.project, json.loads(Path(args.catalog).read_text())))
    elif args.action == "start":
        start(args.root)
    elif args.action == "stop":
        subprocess.run(compose(args.root) + ["--profile", "*", "stop", "--timeout", "5"], check=True)
    else:
        print(json.dumps(control(args.root, {"op": "view"}), indent=2))
