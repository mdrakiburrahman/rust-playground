"""Bounded loopback bridge; effects require an explicitly configured qualified executor."""
from collections import deque
import copy
from dataclasses import dataclass, field
import hmac
from http.cookies import CookieError, SimpleCookie
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import mimetypes
from pathlib import Path
import re
import secrets
import socket
from socketserver import ThreadingMixIn
import threading
import time
from urllib.parse import parse_qs, unquote, urlsplit

from jsonschema.exceptions import ValidationError

from exchange import unique_object
from live_contract import (
    ARM_TTL_NS, Denial, DeploymentFence, REQUEST_BYTES, RESPONSE_BYTES,
    SNAPSHOT_AGE_NS, SessionFence, authorize_command, decode_request, require_origin, validate_packet,
)


OPERATIONS = ("replicas", "retirement", "migration", "exclusion", "admission", "workload", "fault", "repair")
SECRET = re.compile(r"[A-Za-z0-9_-]{32,512}")
COUNTER = re.compile(r"(0|[1-9][0-9]{0,19})")
SESSION_TTL_NS = 900_000_000_000


@dataclass
class Session:
    csrf: str
    deadline: int
    selection: dict | None = None
    requests: deque = field(default_factory=deque)
    last_feed_ns: int = 0
    arm_id: str | None = None
    arm_deadline_ns: int = 0


class Bridge:
    """Owns volatile browser authority; the observer supplies no authorizing data."""

    def __init__(self, origin, deployment, static_root, ledger, executor=None):
        self.origin = require_origin(origin)
        self.host = urlsplit(origin).netloc
        self.identity = {"id": "local-bridge", "incarnation": secrets.token_hex(16)}
        self.deployment = deployment
        self.static_root = Path(static_root).resolve(strict=True)
        if not (self.static_root / "index.html").is_file():
            raise ValueError("production frontend index is required")
        self.ledger = ledger
        self.executor = executor
        self.lock = threading.RLock()
        self.sessions = {}
        self.grant = secrets.token_urlsafe(32)
        self.grant_deadline = time.monotonic_ns() + 300_000_000_000
        self.history = deque(maxlen=64)
        self.revision = self.ledger.bind_deployment(deployment)
        self.last_resources = None

    @staticmethod
    def invalidate(session):
        session.selection = None
        session.arm_id = None
        session.arm_deadline_ns = 0

    def boundary(self, headers, method):
        if headers.get("Host") != self.host:
            raise Denial("origin")
        if headers.get("Sec-Fetch-Site") != "same-origin":
            raise Denial("origin")
        if headers.get("Sec-Fetch-Mode") not in ("cors", "same-origin") or headers.get("Sec-Fetch-Dest") != "empty":
            raise Denial("origin")
        if method == "POST" and headers.get("Origin") != self.origin:
            raise Denial("origin")
        if headers.get("Origin") not in (None, self.origin):
            raise Denial("origin")

    def authenticate(self, headers, post=False):
        cookie = SimpleCookie()
        try:
            cookie.load(headers.get("Cookie", ""))
        except CookieError as error:
            # Cookie parser errors are invalid transport, never an authenticated fallback.
            raise Denial("unauthenticated") from error
        value = cookie.get("arrowcol_live")
        token = value.value if value is not None else ""
        if not SECRET.fullmatch(token):
            raise Denial("unauthenticated")
        now = time.monotonic_ns()
        with self.lock:
            self.sessions = {key: session for key, session in self.sessions.items() if session.deadline > now}
            session = self.sessions.get(token)
            if session is None:
                raise Denial("unauthenticated")
            csrf = headers.get("X-Arrowcol-CSRF", "")
            if post and (not SECRET.fullmatch(csrf) or not hmac.compare_digest(csrf, session.csrf)):
                raise Denial("csrf")
            while session.requests and now - session.requests[0] >= 1_000_000_000:
                session.requests.popleft()
            if len(session.requests) >= 12:
                raise Denial("limit")
            session.requests.append(now)
            return token, session

    def bootstrap(self, body):
        now = time.monotonic_ns()
        if not isinstance(body, dict) or set(body) != {"grant"} or not isinstance(body["grant"], str):
            raise Denial("invalid")
        supplied = body["grant"]
        with self.lock:
            if (not SECRET.fullmatch(supplied) or self.grant is None or now >= self.grant_deadline
                    or not hmac.compare_digest(supplied, self.grant)):
                raise Denial("unauthenticated")
            self.grant = None
            token = secrets.token_urlsafe(32)
            csrf = secrets.token_urlsafe(32)
            self.sessions[token] = Session(csrf, now + SESSION_TTL_NS)
        return {"csrf": csrf}, token

    def publish(self, replicas, tables, receipts, metrics, missing, connected=True, observed_ns=None, dropped=0):
        with self.lock:
            now = time.monotonic_ns()
            observed_ns = now if observed_ns is None else observed_ns
            age = now - observed_ns
            if age < 0:
                raise ValueError("observation is from another clock")
            resources = [(replica["resource"], replica["writer"]) for replica in replicas]
            revision = self.revision + 1
            snapshot = {
                "kind": "snapshot", "schema_version": 1, "bridge": self.identity,
                "deployment": self.deployment, "revision": str(revision),
                "freshness": {"status": ("fresh" if age <= SNAPSHOT_AGE_NS else "stale") if connected else "unknown",
                              "age_ns": str(age) if connected else None, "maximum_age_ns": str(SNAPSHOT_AGE_NS)},
                "replicas": replicas, "tables": tables, "receipts": receipts, "metrics": metrics,
                "coverage": {"complete": not missing and not dropped,
                             "missing": sorted(set(missing)), "dropped": dropped},
            }
            validate_packet(snapshot)
            self.ledger.publish_revision(self.deployment, self.revision, revision)
            gap = self.history and now - self.history[-1][1] > SNAPSHOT_AGE_NS
            if not connected or gap or self.last_resources is not None and resources != self.last_resources:
                for session in self.sessions.values():
                    self.invalidate(session)
            self.last_resources = copy.deepcopy(resources)
            self.revision = revision
            self.history.append((copy.deepcopy(snapshot), observed_ns, connected))

    def snapshot(self):
        with self.lock:
            if not self.history:
                raise Denial("disconnected")
            result = self._aged(self.history[-1])
            if result["freshness"]["status"] != "fresh":
                for session in self.sessions.values():
                    self.invalidate(session)
            return result

    def _aged(self, entry):
        value, observed, connected = entry
        result = copy.deepcopy(value)
        age = time.monotonic_ns() - observed
        result["freshness"].update(
            status=("fresh" if age <= SNAPSHOT_AGE_NS else "stale") if connected else "unknown",
            age_ns=str(age) if connected else None,
        )
        return result

    def capabilities(self):
        with self.lock:
            connected = bool(self.history and self.snapshot()["freshness"]["status"] == "fresh")
            qualified = self.executor is not None and not self.executor.failed.is_set()
        supported = self.executor.operations if qualified else frozenset()
        return {
            "kind": "capabilities", "schema_version": 1, "bridge": self.identity,
            "connected": connected, "qualified": qualified, "deployments": [self.deployment],
            "operations": [{"operation": operation, "supported": connected and operation in supported,
                            "reason": ("qualified" if operation in supported else "unsupported") if connected else "disconnected",
                            "maximum": getattr(self.executor, "maximums", {}).get(operation, 1)
                            if operation in supported else None}
                           for operation in OPERATIONS],
            "workload_presets": copy.deepcopy(getattr(self.executor, "presets", [])) if qualified and connected else [],
            "limits": {"request_bytes": REQUEST_BYTES, "response_bytes": RESPONSE_BYTES, "feed_items": 64,
                       "arm_ttl_ms": 30000, "snapshot_max_age_ms": 5000},
        }

    def feed(self, after):
        if not COUNTER.fullmatch(after):
            raise Denial("invalid")
        with self.lock:
            if not self.history:
                raise Denial("disconnected")
            cursor = int(after)
            first = int(self.history[0][0]["revision"])
            gap = cursor < first - 1 or cursor > self.revision
            result = {
                "kind": "feed", "schema_version": 1, "bridge": self.identity, "deployment": self.deployment,
                "after": after, "next": after, "gap": gap, "resnapshot": gap, "items": [],
            }
            if not gap:
                for entry in self.history:
                    if int(entry[0]["revision"]) <= cursor:
                        continue
                    item = self._aged(entry)
                    candidate = {**result, "items": [*result["items"], item], "next": item["revision"]}
                    if len(json.dumps(candidate, allow_nan=False).encode()) > RESPONSE_BYTES:
                        if not result["items"]:
                            gap = True
                            result.update(gap=True, resnapshot=True)
                        break
                    result = candidate
            if gap:
                for session in self.sessions.values():
                    self.invalidate(session)
            return result

    def select(self, packet, session):
        with self.lock:
            self.invalidate(session)
            snapshot = self.snapshot()
            if packet["deployment"] != self.deployment:
                raise Denial("wrong-target")
            if packet["revision"] != snapshot["revision"] or snapshot["freshness"]["status"] != "fresh":
                raise Denial("stale")
            session.selection = {
                "kind": "selection", "schema_version": 1, "selection_id": secrets.token_hex(16),
                "deployment": self.deployment, "revision": packet["revision"], "armed": False,
            }
            return copy.deepcopy(session.selection)

    def arm(self, packet, session):
        with self.lock:
            if self.executor is None:
                raise Denial("unsupported")
            snapshot = self.snapshot()
            if not self.capabilities()["qualified"]:
                raise Denial("unqualified")
            if packet["deployment"] != self.deployment:
                raise Denial("wrong-target")
            if (session.selection is None
                    or packet["selection_id"] != session.selection["selection_id"]):
                raise Denial("disarmed")
            if snapshot["freshness"]["status"] != "fresh" or packet["revision"] != snapshot["revision"]:
                raise Denial("stale")
            session.arm_id = secrets.token_hex(16)
            session.arm_deadline_ns = time.monotonic_ns() + ARM_TTL_NS
            return {"kind": "armed", "schema_version": 1,
                    "selection_id": session.selection["selection_id"], "arm_id": session.arm_id,
                    "deployment": self.deployment, "revision": packet["revision"], "ttl_ms": 30000}

    def command(self, packet, session):
        with self.lock:
            if self.executor is None:
                raise Denial("unsupported")
            deployment, fence = self.command_fences(session)
            return self.executor.submit(packet, deployment, fence, self.origin,
                                        recheck=lambda: self.authorize_effect(packet, session))

    def authorize_effect(self, packet, session):
        with self.lock:
            if (session.deadline <= time.monotonic_ns()
                    or not any(current is session for current in self.sessions.values())):
                raise Denial("disarmed")
            deployment, fence = self.command_fences(session)
            authorize_command(packet, deployment, fence, self.origin)

    def command_fences(self, session):
        with self.lock:
            snapshot = self.snapshot()
            capabilities = self.capabilities()
            now = time.monotonic_ns()
            age = snapshot["freshness"]["age_ns"]
            targets = {
                replica["resource"]["id"]: {
                    "resource": replica["resource"], "writer": replica["writer"],
                    "policy_revision": replica["policy_revision"],
                }
                for replica in snapshot["replicas"]
                if replica["status"] not in ("failed", "retired")
            }
            deployment = DeploymentFence(
                self.deployment, snapshot["revision"], None if age is None else now - int(age),
                capabilities["connected"], capabilities["qualified"],
                frozenset(item["operation"] for item in capabilities["operations"] if item["supported"]), targets,
            )
            fence = SessionFence(
                self.origin, True, True, "live", self.deployment,
                session.selection["selection_id"] if session.selection else None,
                session.arm_id, session.arm_deadline_ns,
            )
            return deployment, fence

    def dispatch(self, method, path, headers, body=None):
        self.boundary(headers, method)
        parsed = urlsplit(path)
        if parsed.fragment or parsed.scheme or parsed.netloc:
            raise Denial("invalid")
        route = parsed.path
        if method == "POST" and route == "/api/live/v1/session":
            if parsed.query:
                raise Denial("invalid")
            return self.bootstrap(body)
        token, session = self.authenticate(headers, method == "POST")
        if method == "GET":
            if route == "/api/live/v1/session" and not parsed.query:
                with self.lock:
                    self.invalidate(session)
                return {"csrf": session.csrf}, None
            if route == "/api/live/v1/capabilities" and not parsed.query:
                return self.capabilities(), None
            if route == "/api/live/v1/snapshot" and not parsed.query:
                return self.snapshot(), None
            if route == "/api/live/v1/feed":
                query = parse_qs(parsed.query, keep_blank_values=True, max_num_fields=1)
                if set(query) != {"after"} or len(query["after"]) != 1:
                    raise Denial("invalid")
                now = time.monotonic_ns()
                with self.lock:
                    if session.last_feed_ns and now - session.last_feed_ns < 1_000_000_000:
                        raise Denial("limit")
                    session.last_feed_ns = now
                return self.feed(query["after"][0]), None
            operation = re.fullmatch(r"/api/live/v1/operations/([A-Za-z0-9][A-Za-z0-9_.:-]{0,127})", route)
            if operation and not parsed.query:
                if self.executor is not None:
                    return self.executor.refresh(operation[1], self.deployment), None
                return self.ledger.get(operation[1], self.deployment), None
            raise Denial("unsupported")
        if parsed.query:
            raise Denial("invalid")
        if route == "/api/live/v1/logout":
            if body != {}:
                raise Denial("invalid")
            with self.lock:
                self.sessions.pop(token, None)
            return {}, ""
        packet = decode_request(json.dumps(body, allow_nan=False).encode())
        expected = {
            "/api/live/v1/selection": "select", "/api/live/v1/arm": "arm",
            "/api/live/v1/disarm": "disarm", "/api/live/v1/commands": "command",
        }.get(route)
        if packet["kind"] != expected:
            raise Denial("invalid")
        if expected == "select":
            return self.select(packet, session), None
        if expected == "disarm":
            with self.lock:
                session.arm_id = None
                session.arm_deadline_ns = 0
                if session.selection is None or packet["selection_id"] != session.selection["selection_id"]:
                    raise Denial("disarmed")
                return copy.deepcopy(session.selection), None
        if expected == "arm":
            return self.arm(packet, session), None
        if expected == "command":
            return self.command(packet, session), None
        raise Denial("unsupported")


class BoundedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    request_queue_size = 8
    allow_reuse_address = True

    def __init__(self, address, bridge):
        if address[0] != "127.0.0.1":
            raise ValueError("bridge must bind IPv4 loopback")
        self.slots = threading.BoundedSemaphore(8)
        self.bridge = bridge
        super().__init__(address, Handler)

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            request.close()
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()

    def handle_error(self, request, client_address):
        # Never log request headers, paths, cookies or bootstrap bodies.
        print("live bridge transport failure", flush=True)


class Handler(BaseHTTPRequestHandler):
    server_version = "ArrowcolLocalBridge/1"
    sys_version = ""

    def setup(self):
        self.request.settimeout(5)
        super().setup()

    def log_message(self, *_):
        pass

    def do_GET(self):
        self.handle_request("GET")

    def do_POST(self):
        self.handle_request("POST")

    def handle_request(self, method):
        try:
            if len(self.path) > 1024 or len(self.headers.as_bytes()) > 16384:
                raise Denial("limit")
            if len(self.headers.get_all("Host", [])) != 1:
                raise Denial("origin")
            for name in ("Origin", "Sec-Fetch-Site", "Sec-Fetch-Mode", "Sec-Fetch-Dest",
                         "Cookie", "X-Arrowcol-CSRF", "Content-Type"):
                if len(self.headers.get_all(name, [])) > 1:
                    raise Denial("invalid")
            if not self.path.startswith("/api/"):
                if method != "GET" or self.headers.get("Host") != self.server.bridge.host:
                    raise Denial("origin")
                self.serve_static()
                return
            self.server.bridge.boundary(self.headers, method)
            body = None
            if self.headers.get("Transfer-Encoding") is not None:
                raise Denial("invalid")
            if method == "POST":
                lengths = self.headers.get_all("Content-Length", [])
                if len(lengths) != 1 or not lengths[0].isdigit():
                    raise Denial("invalid")
                length = int(lengths[0])
                if length > REQUEST_BYTES:
                    raise Denial("limit")
                if self.headers.get("Content-Type") != "application/json":
                    raise Denial("invalid")
                raw = self.rfile.read(length)
                if len(raw) != length:
                    raise Denial("invalid")
                body = json.loads(raw, object_pairs_hook=unique_object,
                                  parse_constant=lambda _: (_ for _ in ()).throw(Denial("invalid")))
                self.check_depth(body)
            value, cookie = self.server.bridge.dispatch(method, self.path, self.headers, body)
            if "kind" in value:
                validate_packet(value)
            data = json.dumps(value, allow_nan=False, separators=(",", ":")).encode()
            if len(data) > RESPONSE_BYTES:
                raise Denial("limit")
            self.reply(200, data, "application/json", cookie)
        except Denial as error:
            status = {"invalid": 400, "unauthenticated": 401, "origin": 403, "csrf": 403, "disarmed": 403,
                      "stale": 409, "wrong-target": 409, "conflict": 409, "unknown-operation": 404,
                      "limit": 413}.get(error.code, 503)
            self.reply(status, json.dumps(error.packet()).encode(), "application/json")
        except (ValueError, ValidationError, RecursionError):
            self.reply(400, json.dumps(Denial("invalid").packet()).encode(), "application/json")
        except (BrokenPipeError, ConnectionResetError, socket.timeout):
            print("live bridge request disconnected", flush=True)
        except OSError:
            print("live bridge source unavailable", flush=True)
            self.reply(503, json.dumps(Denial("disconnected").packet()).encode(), "application/json")

    @classmethod
    def check_depth(cls, value, depth=0):
        if depth > 16:
            raise Denial("limit")
        if isinstance(value, dict):
            for child in value.values():
                cls.check_depth(child, depth + 1)
        elif isinstance(value, list):
            for child in value:
                cls.check_depth(child, depth + 1)

    def reply(self, status, data, content_type, cookie=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Cross-Origin-Resource-Policy", "same-origin")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", "frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        if cookie is not None:
            value = (f"arrowcol_live={cookie}; HttpOnly; SameSite=Strict; Path=/api/live/v1; "
                     f"Max-Age={900 if cookie else 0}")
            self.send_header("Set-Cookie", value)
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True

    def serve_static(self):
        parsed = urlsplit(self.path)
        if parsed.query or parsed.fragment:
            raise Denial("invalid")
        name = unquote(parsed.path, errors="strict").lstrip("/") or "index.html"
        parts = Path(name).parts
        if any(part.startswith(".") or "\\" in part or ":" in part for part in parts):
            raise Denial("invalid")
        if Path(name).suffix.lower() not in {
            ".html", ".js", ".css", ".svg", ".png", ".webp", ".jpg", ".jpeg", ".gif",
            ".ico", ".woff", ".woff2", ".ttf", ".wasm", ".glb", ".gltf", ".mp3", ".ogg", ".wav",
        }:
            raise Denial("invalid")
        root = self.server.bridge.static_root
        path = root
        for part in parts:
            path = path / part
            if path.is_symlink():
                raise Denial("invalid")
        if not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
            raise Denial("invalid")
        with path.open("rb") as stream:
            data = stream.read(8 * 1024 * 1024 + 1)
        if len(data) > 8 * 1024 * 1024:
            raise Denial("limit")
        self.reply(200, data, mimetypes.guess_type(path)[0] or "application/octet-stream")
