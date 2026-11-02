import contextlib
import io
import json
import threading
import unittest
import urllib.error
import urllib.request
from types import SimpleNamespace
from unittest.mock import patch

import torrentctl
from management import (
    APIClient,
    CommandDispatcher,
    ManagementError,
    ManagementService,
    make_handler,
    normalize_source,
    validate_management_config,
)
from http.server import ThreadingHTTPServer


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
        SimpleNamespace(), registry or FakeRegistry(), add_callback,
        instance_id="node-1", mode="download",
    )


class ServiceTests(unittest.TestCase):
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


class ConfigTests(unittest.TestCase):
    def test_disabled_listener_needs_no_token(self):
        validate_management_config("0.0.0.0", 0, "", False)

    def test_enabled_listener_requires_token(self):
        with self.assertRaisesRegex(ValueError, "TOKEN"):
            validate_management_config("127.0.0.1", 8080, "", False)

    def test_remote_listener_requires_explicit_opt_in(self):
        with self.assertRaisesRegex(ValueError, "allow-remote"):
            validate_management_config("0.0.0.0", 8080, "secret", False)
        validate_management_config("0.0.0.0", 8080, "secret", True)


class ImmediateDispatcher:
    def __init__(self, service):
        self.service = service

    def invoke(self, method, *args, timeout=5):
        return getattr(self.service, method)(*args)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        self.registry = FakeRegistry({HASH: FakeHandle()})
        service = make_service(self.registry)
        self.server = ThreadingHTTPServer(
            ("127.0.0.1", 0), make_handler(ImmediateDispatcher(service), "secret")
        )
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def request(self, method, path, token="secret", body=None):
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as exc:
            return exc.code, json.load(exc)

    def test_auth_and_error_shape(self):
        status, payload = self.request("GET", "/api/v1/status", token="wrong")
        self.assertEqual(status, 401)
        self.assertEqual(payload["error"]["code"], "unauthorized")

    def test_query_add_action_and_not_found_boundaries(self):
        status, payload = self.request("GET", "/api/v1/torrents")
        self.assertEqual((status, len(payload["torrents"])), (200, 1))

        status, payload = self.request("POST", "/api/v1/torrents", body={"source": OTHER_HASH})
        self.assertEqual((status, payload["status"]), (202, "accepted"))

        status, payload = self.request("POST", f"/api/v1/torrents/{HASH}/pause", body={})
        self.assertEqual((status, payload["affected"]), (200, 1))

        status, payload = self.request("GET", "/api/v1/unknown")
        self.assertEqual((status, payload["error"]["code"]), (404, "not_found"))

    def test_duplicate_capacity_and_validation_http_errors(self):
        status, payload = self.request("POST", "/api/v1/torrents", body={"source": HASH})
        self.assertEqual((status, payload["error"]["code"]), (409, "duplicate"))

        self.registry.maxsize = 1
        status, payload = self.request("POST", "/api/v1/torrents", body={"source": OTHER_HASH})
        self.assertEqual((status, payload["error"]["code"]), (409, "capacity_reached"))

        status, payload = self.request("POST", "/api/v1/torrents", body={"source": "/etc/passwd"})
        self.assertEqual((status, payload["error"]["code"]), (400, "invalid_source"))


class CLITests(unittest.TestCase):
    def test_json_success(self):
        payload = {"instance_id": "node-1", "mode": "download", "live_torrent_count": 0, "capacity": 2}
        output = io.StringIO()
        with patch.object(APIClient, "request", return_value=payload), contextlib.redirect_stdout(output):
            code = torrentctl.main(["--address", "http://localhost", "--token", "x", "--json", "status"])
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(output.getvalue()), payload)

    def test_api_error_is_stderr_and_stable_exit_code(self):
        error = io.StringIO()
        with patch.object(APIClient, "request", side_effect=ManagementError("not_found", "missing", 404)), contextlib.redirect_stderr(error):
            code = torrentctl.main(["--address", "http://localhost", "--token", "x", "get", HASH])
        self.assertEqual(code, 3)
        self.assertIn("not_found: missing", error.getvalue())


if __name__ == "__main__":
    unittest.main()
