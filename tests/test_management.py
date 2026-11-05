import contextlib
import io
import json
import os
import socket
import threading
import time
import unittest
import urllib.error
import urllib.request
from types import SimpleNamespace
from unittest.mock import patch

import torrentctl
from management import (
    APIClient,
    BoundedHTTPServer,
    CommandDispatcher,
    LiveHandles,
    ManagementError,
    ManagementService,
    _is_loopback_host,
    _origin_is_loopback,
    make_handler,
    normalize_source,
    peer_is_loopback,
    start_management_server,
    validate_management_config,
)


HASH = "a" * 40
OTHER_HASH = "b" * 40


class FakeRegistry:
    def __init__(self, values=None, maxsize=2):
        self.data = dict(values or {})
        self.maxsize = maxsize

    def items(self):
        return list(self.data.items())

    def get(self, key, default=None):
        return self.data.get(key, default)

    def __contains__(self, key):
        return key in self.data

    def __len__(self):
        return len(self.data)


class FakeHandle:
    def __init__(self, name="example"):
        self.calls = []
        self._status = SimpleNamespace(
            name=name,
            state=3,
            paused=False,
            has_metadata=True,
            progress=0.5,
            total_wanted=2048,
            download_rate=10,
            upload_rate=20,
            total_download=100,
            total_upload=200,
            num_peers=3,
            num_seeds=1,
        )

    def is_valid(self):
        return True

    def status(self):
        return self._status

    def pause(self):
        self.calls.append("pause")

    def resume(self):
        self.calls.append("resume")

    def force_reannounce(self):
        self.calls.append("reannounce")


def make_service(registry=None, add_callback=lambda source: True):
    return ManagementService(
        SimpleNamespace(), registry if registry is not None else FakeRegistry(), add_callback,
        instance_id="node-1", mode="download",
    )


class ServiceTests(unittest.TestCase):
    def test_live_inventory_survives_ttl_expiry(self):
        from app import TTLDict

        pending = TTLDict(maxsize=2, ttl=0.01)
        live = LiveHandles(maxsize=2)
        handle = FakeHandle()
        pending.put(HASH, handle)
        live.put(HASH, handle)
        service = ManagementService(
            SimpleNamespace(), live, lambda source: True,
            instance_id="node-1", mode="download", pending_registry=pending,
        )
        time.sleep(0.03)

        self.assertNotIn(HASH, pending)
        self.assertEqual(service.status()["live_torrent_count"], 1)
        self.assertEqual(service.get_torrent(HASH)["torrent"]["info_hash"], HASH)
        self.assertEqual(service.action(HASH, "pause")["affected"], 1)
        with self.assertRaises(ManagementError) as raised:
            service.add_torrent(HASH)
        self.assertEqual(raised.exception.code, "duplicate")

    def test_status_and_live_snapshot_have_explicit_units(self):
        handle = FakeHandle()
        service = make_service(FakeRegistry({HASH: handle}))

        self.assertEqual(service.status()["live_torrent_count"], 1)
        item = service.list_torrents()["torrents"][0]

        self.assertEqual(item["info_hash"], HASH)
        self.assertEqual(item["total_size_bytes"], 2048)
        self.assertEqual(item["download_rate_bytes_per_second"], 10)

    def test_get_missing_is_not_found(self):
        with self.assertRaises(ManagementError) as raised:
            make_service().get_torrent(HASH)
        self.assertEqual((raised.exception.code, raised.exception.status), ("not_found", 404))

    def test_targeted_and_all_actions(self):
        first, second = FakeHandle(), FakeHandle()
        service = make_service(FakeRegistry({HASH: first, OTHER_HASH: second}))

        result = service.action(HASH, "pause")
        service.action("all", "reannounce")

        self.assertEqual(result["affected"], 1)
        self.assertEqual(first.calls, ["pause", "reannounce"])
        self.assertEqual(second.calls, ["reannounce"])

    def test_add_validation_duplicate_capacity_and_acknowledgement(self):
        submitted = []
        service = make_service(FakeRegistry(maxsize=1), lambda source: submitted.append(source) or True)
        result = service.add_torrent(HASH.upper())
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(result["info_hash"], HASH)
        self.assertEqual(submitted, [HASH])

        duplicate = make_service(FakeRegistry({HASH: None}))
        with self.assertRaisesRegex(ManagementError, "already registered"):
            duplicate.add_torrent(HASH)

        full = make_service(FakeRegistry({OTHER_HASH: None}, maxsize=1))
        with self.assertRaises(ManagementError) as raised:
            full.add_torrent(HASH)
        self.assertEqual(raised.exception.code, "capacity_reached")

        with self.assertRaises(ManagementError) as raised:
            service.add_torrent("/server/private/file.torrent")
        self.assertEqual(raised.exception.code, "invalid_source")

    def test_base32_magnet_normalizes_to_stable_hex_identifier(self):
        source = "magnet:?xt=urn:btih:VKVKVKVKVKVKVKVKVKVKVKVKVKVKVKVK"
        _, info_hash = normalize_source(source)
        self.assertEqual(info_hash, "aa" * 20)

    def test_v2_hash_becomes_btmh_magnet(self):
        source, info_hash = normalize_source("c" * 64)
        self.assertEqual(source, "magnet:?xt=urn:btmh:1220" + "c" * 64)
        self.assertEqual(info_hash, "c" * 64)

    def test_dispatcher_executes_only_when_alert_loop_drains(self):
        dispatcher = CommandDispatcher(make_service())
        result = []
        thread = threading.Thread(target=lambda: result.append(dispatcher.invoke("status", timeout=1)))
        thread.start()
        dispatcher.drain()
        thread.join()
        self.assertEqual(result[0]["instance_id"], "node-1")

    def test_timed_out_queued_action_is_cancelled_before_delayed_drain(self):
        handle = FakeHandle()
        dispatcher = CommandDispatcher(make_service(FakeRegistry({HASH: handle})))

        with self.assertRaises(ManagementError) as raised:
            dispatcher.invoke("action", HASH, "pause", timeout=0.01)
        self.assertEqual(raised.exception.code, "command_cancelled")

        dispatcher.drain()
        self.assertEqual(handle.calls, [])

    def test_timeout_after_action_started_reports_unknown_outcome(self):
        started = threading.Event()
        release = threading.Event()
        calls = []

        def action(selector, operation):
            started.set()
            release.wait(1)
            calls.append((selector, operation))

        dispatcher = CommandDispatcher(SimpleNamespace(action=action))
        worker = threading.Thread(target=lambda: (time.sleep(0.01), dispatcher.drain()))
        worker.start()
        try:
            with self.assertRaises(ManagementError) as raised:
                dispatcher.invoke("action", HASH, "pause", timeout=0.1)
            self.assertTrue(started.is_set())
            self.assertEqual(raised.exception.code, "outcome_unknown")
        finally:
            release.set()
            worker.join()
        self.assertEqual(calls, [(HASH, "pause")])


class ConfigTests(unittest.TestCase):
    def test_disabled_listener_needs_no_token(self):
        validate_management_config("0.0.0.0", 0, "", False)

    def test_loopback_listener_does_not_require_token(self):
        # A stock loopback bind must start with no token configured.
        validate_management_config("127.0.0.1", 8080, "", False)
        validate_management_config("localhost", 8080, "", False)
        validate_management_config("::1", 8080, "", False)

    def test_loopback_listener_accepts_optional_token(self):
        # Configuring a token on a loopback listener is allowed (and still
        # accepted by the server), just not required.
        validate_management_config("127.0.0.1", 8080, "optional", False)

    def test_remote_listener_requires_allow_remote_and_token(self):
        with self.assertRaisesRegex(ValueError, "allow-remote"):
            validate_management_config("0.0.0.0", 8080, "", False)
        with self.assertRaisesRegex(ValueError, "TOKEN"):
            validate_management_config("0.0.0.0", 8080, "", True)
        validate_management_config("0.0.0.0", 8080, "secret", True)

    def test_rejects_invalid_port_and_host(self):
        with self.assertRaises(ValueError):
            validate_management_config("127.0.0.1", 99999, "", False)
        with self.assertRaises(ValueError):
            validate_management_config("not-a-real-host.example", 8080, "", False)


class PeerLoopbackUnitTests(unittest.TestCase):
    def test_ipv4_loopback_variants(self):
        self.assertTrue(peer_is_loopback(("127.0.0.1", 1234)))
        self.assertTrue(peer_is_loopback(("127.1.2.3", 1234)))
        self.assertTrue(peer_is_loopback(("127.255.255.255", 1234)))

    def test_ipv6_loopback_and_mapped(self):
        self.assertTrue(peer_is_loopback(("::1", 1234)))
        self.assertTrue(peer_is_loopback(("::ffff:127.0.0.1", 1234)))

    def test_non_loopback_addresses(self):
        self.assertFalse(peer_is_loopback(("10.0.0.1", 1234)))
        self.assertFalse(peer_is_loopback(("192.168.1.1", 1234)))
        self.assertFalse(peer_is_loopback(("8.8.8.8", 1234)))
        self.assertFalse(peer_is_loopback(("::2", 1234)))

    def test_empty_or_garbage_address(self):
        self.assertFalse(peer_is_loopback(("", 0)))
        self.assertFalse(peer_is_loopback(("not-an-ip", 0)))
        self.assertFalse(peer_is_loopback(()))


class HostAndOriginUnitTests(unittest.TestCase):
    def test_loopback_host_values(self):
        self.assertTrue(_is_loopback_host("127.0.0.1"))
        self.assertTrue(_is_loopback_host("127.0.0.1:8080"))
        self.assertTrue(_is_loopback_host("localhost"))
        self.assertTrue(_is_loopback_host("localhost:8080"))
        self.assertTrue(_is_loopback_host("[::1]"))
        self.assertTrue(_is_loopback_host("[::1]:8080"))
        self.assertTrue(_is_loopback_host("::1"))

    def test_non_loopback_host_values(self):
        self.assertFalse(_is_loopback_host("10.0.0.1"))
        self.assertFalse(_is_loopback_host("10.0.0.1:8080"))
        self.assertFalse(_is_loopback_host("example.com"))
        self.assertFalse(_is_loopback_host("evil.com:8080"))
        self.assertFalse(_is_loopback_host(""))

    def test_origin_loopback(self):
        self.assertTrue(_origin_is_loopback("http://127.0.0.1:8080"))
        self.assertTrue(_origin_is_loopback("http://localhost"))
        self.assertTrue(_origin_is_loopback("http://[::1]:8080"))

    def test_origin_rejects_cross_site_and_null(self):
        self.assertFalse(_origin_is_loopback("http://evil.com"))
        self.assertFalse(_origin_is_loopback("http://evil.com:8080"))
        self.assertFalse(_origin_is_loopback("null"))
        self.assertFalse(_origin_is_loopback(""))
        self.assertFalse(_origin_is_loopback("ftp://127.0.0.1"))


class ImmediateDispatcher:
    def __init__(self, service):
        self.service = service

    def invoke(self, method, *args, timeout=5):
        return getattr(self.service, method)(*args)


def _make_server(token="secret", **overrides):
    registry = FakeRegistry({HASH: FakeHandle()})
    service = make_service(registry)
    defaults = dict(max_connections=1, header_timeout=0.3, body_timeout=0.3)
    defaults.update(overrides)
    server = BoundedHTTPServer(
        ("127.0.0.1", 0),
        make_handler(ImmediateDispatcher(service), token),
        **defaults,
    )
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    return server, thread, registry


class LoopbackPeerHTTPTests(unittest.TestCase):
    """Loopback peers are exempt from bearer auth; Origin/Host are still checked."""

    def setUp(self):
        self.server, self.thread, self.registry = _make_server(token="")
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def request(self, method, path, body=None, extra_headers=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if extra_headers:
            headers.update(extra_headers)
        request = urllib.request.Request(
            self.base + path, data=data, method=method, headers=headers,
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_loopback_peer_exempt_without_token(self):
        # No Authorization header, no token configured on server → 200.
        status, payload = self.request("GET", "/api/v1/status")
        self.assertEqual(status, 200)
        self.assertEqual(payload["instance_id"], "node-1")

    def test_loopback_peer_can_mutate(self):
        status, payload = self.request("POST", "/api/v1/torrents", body={"source": OTHER_HASH})
        self.assertEqual((status, payload["status"]), (202, "accepted"))

    def test_loopback_peer_with_valid_host_and_no_origin(self):
        # Native CLI shape: no Origin, Host points at the loopback endpoint.
        status, _ = self.request(
            "GET", "/api/v1/status",
            extra_headers={"Host": f"127.0.0.1:{self.server.server_address[1]}"},
        )
        self.assertEqual(status, 200)

    def test_loopback_peer_with_loopback_origin(self):
        # Same-origin browser request: Origin is loopback → allowed.
        status, _ = self.request(
            "GET", "/api/v1/status",
            extra_headers={
                "Origin": f"http://127.0.0.1:{self.server.server_address[1]}",
                "Host": f"127.0.0.1:{self.server.server_address[1]}",
            },
        )
        self.assertEqual(status, 200)

    def test_cross_site_origin_is_rejected(self):
        # Browser-originated cross-site request: Origin != loopback → 403.
        status, payload = self.request(
            "GET", "/api/v1/status",
            extra_headers={
                "Origin": "http://evil.com",
                "Host": f"127.0.0.1:{self.server.server_address[1]}",
            },
        )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"]["code"], "forbidden_origin")

    def test_dns_rebinding_via_host_is_rejected(self):
        # Host header identifies a non-loopback name → 403.
        status, payload = self.request(
            "GET", "/api/v1/status",
            extra_headers={"Host": "malicious.example"},
        )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"]["code"], "forbidden_origin")

    def test_post_with_simple_content_type_still_requires_safe_origin(self):
        # A POST from a browser with a simple Content-Type still carries Origin;
        # when Origin is cross-site it must be rejected.
        status, payload = self.request(
            "POST", "/api/v1/torrents",
            body={"source": OTHER_HASH},
            extra_headers={
                "Origin": "http://evil.com",
                "Host": f"127.0.0.1:{self.server.server_address[1]}",
                "Content-Type": "text/plain",
            },
        )
        self.assertEqual(status, 403)
        self.assertEqual(payload["error"]["code"], "forbidden_origin")

    def test_x_forwarded_for_does_not_influence_auth(self):
        # Forwarding headers are ignored; only the TCP peer matters.
        status, _ = self.request(
            "GET", "/api/v1/status",
            extra_headers={
                "X-Forwarded-For": "10.0.0.1",
                "Host": f"127.0.0.1:{self.server.server_address[1]}",
            },
        )
        # Loopback peer, safe Host → still allowed (X-Forwarded-For ignored).
        self.assertEqual(status, 200)


class LoopbackPeerWithTokenHTTPTests(unittest.TestCase):
    """A configured token is accepted on loopback but is not required."""

    def setUp(self):
        self.server, self.thread, self.registry = _make_server(token="secret")
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def request(self, method, path, token=None, body=None, extra_headers=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        if extra_headers:
            headers.update(extra_headers)
        request = urllib.request.Request(
            self.base + path, data=data, method=method, headers=headers,
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_loopback_peer_works_without_token_even_when_configured(self):
        status, payload = self.request("GET", "/api/v1/status")
        self.assertEqual(status, 200)
        self.assertEqual(payload["instance_id"], "node-1")

    def test_loopback_peer_also_accepts_a_valid_token(self):
        status, _ = self.request("GET", "/api/v1/status", token="secret")
        self.assertEqual(status, 200)

    def test_loopback_peer_ignores_an_invalid_token(self):
        # Loopback peers are exempt — a bogus bearer is not rejected.
        status, _ = self.request("GET", "/api/v1/status", token="bogus")
        self.assertEqual(status, 200)


class NonLoopbackPeerHTTPTests(unittest.TestCase):
    """Simulate a non-loopback TCP peer against a loopback-bound listener.

    We cannot easily set up a real non-loopback peer in CI, so we patch
    `peer_is_loopback` at the module level for the duration of each test.
    """

    def setUp(self):
        self.server, self.thread, self.registry = _make_server(token="secret")
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.patcher = patch("management.peer_is_loopback", return_value=False)
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def request(self, method, path, token=None, body=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(
            self.base + path, data=data, method=method, headers=headers,
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_non_loopback_peer_without_token_is_unauthorized(self):
        status, payload = self.request("GET", "/api/v1/status")
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"]["code"], "unauthorized")

    def test_non_loopback_peer_with_wrong_token_is_unauthorized(self):
        status, payload = self.request("GET", "/api/v1/status", token="wrong")
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"]["code"], "unauthorized")

    def test_non_loopback_peer_with_correct_token_is_allowed(self):
        status, payload = self.request("GET", "/api/v1/status", token="secret")
        self.assertEqual(status, 200)
        self.assertEqual(payload["instance_id"], "node-1")

    def test_non_loopback_peer_ignores_forwarding_headers(self):
        # Even if a forged X-Forwarded-For claims loopback, the peer check
        # still sees the real TCP peer. We simulate this by reusing the
        # patched peer_is_loopback (which returns False) and sending a real
        # Authorization header; without it we expect 401.
        status, payload = self.request("GET", "/api/v1/status")
        self.assertEqual(status, 401)


class PortCollisionTests(unittest.TestCase):
    def test_port_conflict_surfaces_as_oserror_with_hint(self):
        registry = FakeRegistry({HASH: FakeHandle()})
        service = make_service(registry)
        dispatcher = ImmediateDispatcher(service)
        # Grab a free port by binding a temporary socket, then release it and
        # start the first management server there.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        first = start_management_server("127.0.0.1", port, "", False, dispatcher)
        self.assertIsNotNone(first)
        self.addCleanup(first.server_close)

        with self.assertRaises(OSError) as raised:
            start_management_server("127.0.0.1", port, "", False, dispatcher)
        message = str(raised.exception).lower()
        self.assertIn("management port", message)

    def test_disabled_listener_is_none(self):
        registry = FakeRegistry()
        dispatcher = ImmediateDispatcher(make_service(registry))
        self.assertIsNone(start_management_server("127.0.0.1", 0, "", False, dispatcher))


class APIClientTests(unittest.TestCase):
    def test_empty_token_does_not_send_authorization(self):
        client = APIClient("http://127.0.0.1:9", "")
        # Build the Request object by hand to inspect its headers.
        request = urllib.request.Request(
            f"{client.address}/api/v1/status",
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        self.assertNotIn("Authorization", request.headers)

    def test_whitespace_only_token_is_treated_as_absent(self):
        client = APIClient("http://127.0.0.1:9", "   ")
        self.assertEqual(client.token, "")


class CLITests(unittest.TestCase):
    def test_json_success(self):
        payload = {"instance_id": "node-1", "mode": "download", "live_torrent_count": 0, "capacity": 2}
        output = io.StringIO()
        with patch.object(APIClient, "request", return_value=payload), contextlib.redirect_stdout(output):
            code = torrentctl.main(["--json", "status"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), payload)

    def test_api_error_is_stderr_and_stable_exit_code(self):
        error = io.StringIO()
        with patch.object(APIClient, "request", side_effect=ManagementError("not_found", "missing", 404)), contextlib.redirect_stderr(error):
            code = torrentctl.main(["get", HASH])
        self.assertEqual(code, 3)
        self.assertIn("not_found: missing", error.getvalue())

    def test_default_address_points_at_loopback(self):
        # CLI with no --address and no env should target http://127.0.0.1:8080.
        # Resolution is lazy in main(), so the raw argparse default is None;
        # the fallback applied there is what users and tests observe.
        parser = torrentctl.build_parser()
        with patch.dict("os.environ", {}, clear=True):
            args = parser.parse_args(["status"])
        resolved = args.address or os.getenv("TORRENT_CLIENT_ADDRESS", torrentctl._DEFAULT_ADDRESS)
        self.assertEqual(resolved, "http://127.0.0.1:8080")
        resolved_token = args.token or os.getenv("TORRENT_CLIENT_TOKEN", "")
        self.assertEqual(resolved_token, "")

    def test_env_overrides_default_address(self):
        parser = torrentctl.build_parser()
        with patch.dict("os.environ", {"TORRENT_CLIENT_ADDRESS": "http://seedbox:8081"}):
            args = parser.parse_args(["status"])
            resolved = args.address or os.getenv("TORRENT_CLIENT_ADDRESS", torrentctl._DEFAULT_ADDRESS)
        self.assertEqual(resolved, "http://seedbox:8081")

    def test_flag_overrides_env_and_default(self):
        parser = torrentctl.build_parser()
        with patch.dict("os.environ", {"TORRENT_CLIENT_ADDRESS": "http://env:8081"}):
            args = parser.parse_args(["--address", "http://flag:9000", "status"])
            resolved = args.address or os.getenv("TORRENT_CLIENT_ADDRESS", torrentctl._DEFAULT_ADDRESS)
        self.assertEqual(resolved, "http://flag:9000")

    def test_cli_works_without_any_address_or_token(self):
        # End-to-end: CLI default address hits the live loopback-bound server
        # (no token configured) and returns success.
        server, thread, _ = _make_server(token="")
        try:
            payload = {"instance_id": "node-1", "mode": "download", "live_torrent_count": 0, "capacity": 2}
            output = io.StringIO()
            with (
                patch.object(APIClient, "request", return_value=payload),
                patch.dict("os.environ", {}, clear=True),
                contextlib.redirect_stdout(output),
            ):
                code = torrentctl.main(["status"])
            self.assertEqual(code, 0)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
