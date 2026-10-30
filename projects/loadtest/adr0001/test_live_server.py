"""Real bounded HTTP transport with synthetic observations, not live Docker acceptance."""
import http.client
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from jsonschema.exceptions import ValidationError

from live_contract import Denial, validate_packet
from live_ledger import OperationLedger
from live_server import BoundedHTTPServer, Bridge


class LiveHTTPTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        (root / "index.html").write_text("<html>synthetic transport fixture</html>")
        self.ledger = OperationLedger(root / "ledger.sqlite")
        self.deployment = {"id": "test-poc", "incarnation": "test-incarnation"}
        self.bridge = Bridge("http://127.0.0.1:1024", self.deployment, root, self.ledger)
        self.bridge.publish([], [], [], [], ["synthetic-observation"])
        self.server = BoundedHTTPServer(("127.0.0.1", 0), self.bridge)
        self.port = self.server.server_address[1]
        self.bridge.origin = f"http://127.0.0.1:{self.port}"
        self.bridge.host = f"127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.01})
        self.thread.start()
        self.cookie = None
        self.csrf = None

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.ledger.close()
        self.directory.cleanup()

    def request(self, method, path, body=None, headers=None, raw=None):
        values = {"Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors",
                  "Sec-Fetch-Dest": "empty", "Origin": self.bridge.origin}
        if self.cookie:
            values["Cookie"] = self.cookie
        if self.csrf:
            values["X-Arrowcol-CSRF"] = self.csrf
        if body is not None:
            raw = json.dumps(body).encode()
            values["Content-Type"] = "application/json"
        values.update(headers or {})
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        try:
            connection.request(method, path, body=raw, headers=values)
            response = connection.getresponse()
            data = response.read()
            kind = response.getheader("Content-Type")
            return response.status, json.loads(data) if kind == "application/json" else data, dict(response.getheaders())
        finally:
            connection.close()

    def bootstrap(self):
        status, body, headers = self.request("POST", "/api/live/v1/session", {"grant": self.bridge.grant})
        self.assertEqual(status, 200)
        cookie = headers["Set-Cookie"]
        self.assertIn("HttpOnly", cookie)
        self.assertIn("SameSite=Strict", cookie)
        self.assertIn("Path=/api/live/v1", cookie)
        self.cookie = cookie.split(";", 1)[0]
        self.csrf = body["csrf"]
        self.assertEqual(headers["Cache-Control"], "no-store")

    def test_api_is_cookie_authenticated_and_bootstrap_is_one_time(self):
        status, packet, _ = self.request("GET", "/api/live/v1/capabilities")
        self.assertEqual((status, packet["code"]), (401, "unauthenticated"))
        grant = self.bridge.grant
        self.bootstrap()
        status, packet, _ = self.request("POST", "/api/live/v1/session", {"grant": grant})
        self.assertEqual((status, packet["code"]), (401, "unauthenticated"))

    def test_preselection_snapshot_does_not_select_or_arm(self):
        self.bootstrap()
        status, packet, _ = self.request("GET", "/api/live/v1/snapshot")
        self.assertEqual(status, 200)
        validate_packet(packet)
        self.assertEqual(packet["deployment"], self.deployment)
        self.assertTrue(all(session.selection is None for session in self.bridge.sessions.values()))
        status, capabilities, _ = self.request("GET", "/api/live/v1/capabilities")
        self.assertFalse(capabilities["qualified"])
        self.assertFalse(any(operation["supported"] for operation in capabilities["operations"]))

    def selection(self):
        status, packet, _ = self.request("POST", "/api/live/v1/selection", {
            "kind": "select", "schema_version": 1, "deployment": self.deployment,
            "revision": str(self.bridge.revision),
        })
        self.assertEqual(status, 200)
        self.assertFalse(packet["armed"])
        return packet

    def test_reload_resumes_csrf_but_invalidates_selection(self):
        self.bootstrap()
        self.selection()
        status, result, _ = self.request("GET", "/api/live/v1/session")
        self.assertEqual(status, 200)
        self.assertEqual(result["csrf"], self.csrf)
        self.assertTrue(all(session.selection is None for session in self.bridge.sessions.values()))

    def test_origin_host_csrf_and_fetch_metadata_are_independent_denials(self):
        self.bootstrap()
        for headers, method, path, body, code in (
            ({"Origin": "http://localhost:1234"}, "GET", "/api/live/v1/snapshot", None, "origin"),
            ({"Host": "attacker.invalid"}, "GET", "/api/live/v1/snapshot", None, "origin"),
            ({"Sec-Fetch-Site": "cross-site"}, "GET", "/api/live/v1/snapshot", None, "origin"),
            ({"Origin": "null"}, "POST", "/api/live/v1/logout", {}, "origin"),
            ({"X-Arrowcol-CSRF": "invalid"}, "POST", "/api/live/v1/logout", {}, "csrf"),
        ):
            with self.subTest(headers=list(headers)):
                status, packet, _ = self.request(method, path, body, headers)
                self.assertEqual((status, packet["code"]), (403, code))

    def test_stale_wrong_target_and_unknown_operations_are_not_success(self):
        self.bootstrap()
        for deployment, revision, code in (
            (self.deployment, "0", "stale"),
            ({"id": "foreign", "incarnation": "foreign"}, str(self.bridge.revision), "wrong-target"),
        ):
            status, packet, _ = self.request("POST", "/api/live/v1/selection", {
                "kind": "select", "schema_version": 1, "deployment": deployment, "revision": revision,
            })
            self.assertEqual((status, packet["code"]), (409, code))
        status, packet, _ = self.request("GET", "/api/live/v1/operations/unknown")
        self.assertEqual((status, packet["code"]), (404, "unknown-operation"))

    def test_readonly_server_never_arms_and_logout_revokes_cookie(self):
        self.bootstrap()
        selection = self.selection()
        status, packet, _ = self.request("POST", "/api/live/v1/arm", {
            "kind": "arm", "schema_version": 1, "selection_id": selection["selection_id"],
            "deployment": self.deployment, "revision": str(self.bridge.revision), "confirm": "dedicated-local-poc",
        })
        self.assertEqual((status, packet["code"]), (503, "unsupported"))
        self.assertEqual(self.ledger.unresolved(), [])
        status, _, headers = self.request("POST", "/api/live/v1/logout", {})
        self.assertEqual(status, 200)
        self.assertIn("Max-Age=0", headers["Set-Cookie"])
        status, packet, _ = self.request("GET", "/api/live/v1/snapshot")
        self.assertEqual((status, packet["code"]), (401, "unauthenticated"))

    def test_feed_revisions_are_consecutive_and_gaps_require_resnapshot(self):
        self.bootstrap()
        self.selection()
        for _ in range(65):
            self.bridge.publish([], [], [], [], ["synthetic-observation"])
        feed = validate_packet(self.bridge.feed("0"))
        self.assertTrue(feed["gap"] and feed["resnapshot"])
        self.assertEqual(feed["items"], [])
        self.assertTrue(all(session.selection is None for session in self.bridge.sessions.values()))
        feed = validate_packet(self.bridge.feed("64"))
        self.assertEqual([item["revision"] for item in feed["items"]], ["65", "66"])
        self.assertEqual(feed["next"], "66")
        with self.assertRaises(Denial):
            self.bridge.feed("01")

    def test_invalid_observation_cannot_consume_a_feed_revision(self):
        with self.assertRaises(ValidationError):
            self.bridge.publish([], [], [], [], ["invalid missing label"])
        self.bridge.publish([], [], [], [], ["synthetic-observation"])
        feed = validate_packet(self.bridge.feed("1"))
        self.assertEqual([item["revision"] for item in feed["items"]], ["2"])

    def test_snapshot_age_uses_local_elapsed_and_is_not_relabelled_fresh(self):
        entry = self.bridge.history[-1]
        self.bridge.history[-1] = (entry[0], time.monotonic_ns() - 6_000_000_000, True)
        snapshot = validate_packet(self.bridge.snapshot())
        self.assertEqual(snapshot["freshness"]["status"], "stale")
        self.bridge.publish([], [], [], [], ["slow-observation"],
                            observed_ns=time.monotonic_ns() - 6_000_000_000)
        self.assertEqual(self.bridge.snapshot()["freshness"]["status"], "stale")

    def test_resource_replacement_invalidates_even_unarmed_selection(self):
        self.bootstrap()
        self.selection()
        resource = {
            "resource": {"id": "test-resource", "incarnation": "test-process"},
            "role": "gateway", "status": "unknown", "writer": None,
            "policy_revision": None, "applied_policy_revision": None,
            "excluded": None, "block_streams": None, "retiring": None,
        }
        self.bridge.publish([resource], [], [], [], ["synthetic-observation"])
        self.assertTrue(all(session.selection is None for session in self.bridge.sessions.values()))

    def test_expired_session_is_rejected(self):
        self.bootstrap()
        for session in self.bridge.sessions.values():
            session.deadline = 0
        status, packet, _ = self.request("GET", "/api/live/v1/snapshot")
        self.assertEqual((status, packet["code"]), (401, "unauthenticated"))

    def test_duplicate_keys_oversized_and_deep_bodies_are_rejected(self):
        self.bootstrap()
        for raw, expected in (
            (b'{"grant":"a","grant":"b"}', 400),
            (b"x" * 32769, 413),
            (b"[" * 18 + b"0" + b"]" * 18, 413),
        ):
            status, packet, _ = self.request("POST", "/api/live/v1/session",
                                           raw=raw, headers={"Content-Type": "application/json"})
            self.assertEqual(status, expected)
            validate_packet(packet)

    def test_static_frontend_does_not_expose_files_outside_root_or_symlinks(self):
        status, data, _ = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn(b"synthetic transport fixture", data)
        for path in ("/../private", "/%2e%2e/private", "/%5cprivate"):
            status, _, _ = self.request("GET", path)
            self.assertEqual(status, 400)
        link = self.bridge.static_root / "outside"
        link.symlink_to("/etc/passwd")
        status, _, _ = self.request("GET", "/outside")
        self.assertEqual(status, 400)
        status, _, _ = self.request("GET", "/ledger.sqlite")
        self.assertEqual(status, 400)

    def test_static_read_failure_is_explicit_and_does_not_expose_exception(self):
        with patch.object(Path, "open", side_effect=PermissionError("synthetic-private-diagnostic")):
            status, packet, _ = self.request("GET", "/")
        self.assertEqual((status, packet["code"]), (503, "disconnected"))
        self.assertNotIn("synthetic-private-diagnostic", str(packet))

    def test_dropped_observations_cannot_claim_complete_coverage(self):
        self.bridge.publish([], [], [], [], [], dropped=2)
        snapshot = validate_packet(self.bridge.snapshot())
        self.assertEqual(snapshot["coverage"], {"complete": False, "missing": [], "dropped": 2})
        revision = self.bridge.revision
        with self.assertRaises(ValidationError):
            self.bridge.publish([], [], [], [], [], dropped=-1)
        self.assertEqual(self.bridge.revision, revision)


if __name__ == "__main__":
    unittest.main()
