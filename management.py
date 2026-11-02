"""Versioned management service, HTTP transport, and API client."""

from __future__ import annotations

import base64
import hmac
import ipaddress
import json
import queue
import socket
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import Future
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable


class ManagementError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def payload(self) -> dict[str, Any]:
        return {"error": {"code": self.code, "message": self.message}}


def validate_management_config(host: str, port: int, token: str, allow_remote: bool) -> None:
    if port == 0:
        return
    if not 1 <= port <= 65535:
        raise ValueError("management port must be between 1 and 65535, or 0 to disable")
    if not token:
        raise ValueError("MANAGEMENT_TOKEN is required when management HTTP is enabled")
    try:
        loopback = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError(f"invalid management bind address: {host}") from exc
    is_loopback = all(
        ipaddress.ip_address(info[4][0].split("%")[0]).is_loopback for info in loopback
    )
    if not is_loopback and not allow_remote:
        raise ValueError(
            "non-loopback management bind requires --management-allow-remote"
        )


def normalize_source(source: Any) -> tuple[str, str]:
    if not isinstance(source, str) or not source.strip():
        raise ManagementError("invalid_source", "source must be a magnet URI or info hash")
    source = source.strip()
    if len(source) in {40, 64} and all(char in "0123456789abcdefABCDEF" for char in source):
        value = source.lower()
        if len(value) == 64:
            return f"magnet:?xt=urn:btmh:1220{value}", value
        return value, value
    if not source.startswith("magnet:?"):
        raise ManagementError(
            "invalid_source", "v1 accepts only magnet URIs or 40-character info hashes"
        )
    params = urllib.parse.parse_qs(urllib.parse.urlsplit(source).query)
    btih = next(
        (value[9:] for value in params.get("xt", []) if value.lower().startswith("urn:btih:")),
        None,
    )
    btmh = next(
        (value[9:] for value in params.get("xt", []) if value.lower().startswith("urn:btmh:")),
        None,
    )
    if not btih and not btmh:
        raise ManagementError("invalid_source", "magnet URI must contain a btih or SHA-256 btmh identifier")
    try:
        if btih and len(btih) == 32:
            info_hash = base64.b32decode(btih.upper()).hex()
        elif btih and len(btih) == 40:
            int(btih, 16)
            info_hash = btih.lower()
        elif btmh and len(btmh) == 68 and btmh.lower().startswith("1220"):
            int(btmh, 16)
            info_hash = btmh[4:].lower()
        else:
            raise ValueError
    except (ValueError, base64.binascii.Error) as exc:
        raise ManagementError("invalid_source", "magnet info-hash identifier is invalid") from exc
    return source, info_hash


class ManagementService:
    """Fast operations over a live session; invoke on the alert-loop thread."""

    def __init__(
        self,
        session: Any,
        registry: Any,
        add_callback: Callable[[str], bool],
        *,
        instance_id: str,
        mode: str,
    ):
        self.session = session
        self.registry = registry
        self.add_callback = add_callback
        self.instance_id = instance_id
        self.mode = mode

    def _valid_items(self) -> list[tuple[str, Any]]:
        return [
            (info_hash, handle)
            for info_hash, handle in self.registry.items()
            if handle is not None and handle.is_valid()
        ]

    @staticmethod
    def _snapshot(info_hash: str, handle: Any) -> dict[str, Any]:
        status = handle.status()
        state = getattr(status, "state", "unknown")
        try:
            state = int(state)
        except (TypeError, ValueError):
            state = str(state)
        return {
            "info_hash": info_hash,
            "name": getattr(status, "name", ""),
            "state": state,
            "paused": bool(getattr(status, "paused", False)),
            "has_metadata": bool(getattr(status, "has_metadata", False)),
            "progress": float(getattr(status, "progress", 0.0)),
            "total_size_bytes": int(getattr(status, "total_wanted", 0)),
            "download_rate_bytes_per_second": int(getattr(status, "download_rate", 0)),
            "upload_rate_bytes_per_second": int(getattr(status, "upload_rate", 0)),
            "total_download_bytes": int(getattr(status, "total_download", 0)),
            "total_upload_bytes": int(getattr(status, "total_upload", 0)),
            "num_peers": int(getattr(status, "num_peers", 0)),
            "num_seeds": int(getattr(status, "num_seeds", 0)),
        }

    def status(self) -> dict[str, Any]:
        items = self._valid_items()
        return {
            "instance_id": self.instance_id,
            "mode": self.mode,
            "live_torrent_count": len(items),
            "capacity": self.registry.maxsize,
        }

    def list_torrents(self) -> dict[str, Any]:
        torrents = [self._snapshot(info_hash, handle) for info_hash, handle in self._valid_items()]
        return {"instance_id": self.instance_id, "torrents": torrents}

    def get_torrent(self, info_hash: str) -> dict[str, Any]:
        info_hash = info_hash.lower()
        handle = self.registry.get(info_hash)
        if handle is None or not handle.is_valid():
            raise ManagementError("not_found", "live torrent not found", 404)
        return {"instance_id": self.instance_id, "torrent": self._snapshot(info_hash, handle)}

    def add_torrent(self, source: Any) -> dict[str, Any]:
        normalized, info_hash = normalize_source(source)
        if info_hash in self.registry:
            raise ManagementError("duplicate", "torrent is already registered", 409)
        if len(self.registry) >= self.registry.maxsize:
            raise ManagementError("capacity_reached", "live torrent capacity reached", 409)
        try:
            accepted = self.add_callback(normalized)
        except Exception as exc:
            raise ManagementError("add_failed", "libtorrent rejected source", 422) from exc
        if not accepted:
            # A concurrent alert can reserve the same hash between checks.
            if info_hash in self.registry:
                raise ManagementError("duplicate", "torrent is already registered", 409)
            raise ManagementError("capacity_reached", "live torrent capacity reached", 409)
        return {
            "instance_id": self.instance_id,
            "info_hash": info_hash,
            "status": "accepted",
        }

    def action(self, selector: str, action: str) -> dict[str, Any]:
        operations = {"pause": "pause", "resume": "resume", "reannounce": "force_reannounce"}
        if action not in operations:
            raise ManagementError("invalid_action", "unsupported action")
        if selector == "all":
            targets = self._valid_items()
        else:
            selector = selector.lower()
            handle = self.registry.get(selector)
            if handle is None or not handle.is_valid():
                raise ManagementError("not_found", "live torrent not found", 404)
            targets = [(selector, handle)]
        for _, handle in targets:
            getattr(handle, operations[action])()
        return {
            "instance_id": self.instance_id,
            "action": action,
            "selector": selector,
            "affected": len(targets),
        }


@dataclass
class _Command:
    method: str
    args: tuple[Any, ...]
    future: Future


class CommandDispatcher:
    """Moves HTTP work onto the alert loop without blocking on client I/O."""

    def __init__(self, service: ManagementService, max_pending: int = 128):
        self.service = service
        self.commands: queue.Queue[_Command] = queue.Queue(maxsize=max_pending)

    def invoke(self, method: str, *args: Any, timeout: float = 5.0) -> Any:
        future: Future = Future()
        try:
            self.commands.put_nowait(_Command(method, args, future))
        except queue.Full as exc:
            raise ManagementError("service_unavailable", "management command queue is full", 503) from exc
        try:
            return future.result(timeout=timeout)
        except TimeoutError as exc:
            raise ManagementError("service_unavailable", "management command timed out", 503) from exc

    def drain(self, max_commands: int = 32) -> None:
        for _ in range(max_commands):
            try:
                command = self.commands.get_nowait()
            except queue.Empty:
                return
            try:
                command.future.set_result(getattr(self.service, command.method)(*command.args))
            except Exception as exc:
                command.future.set_exception(exc)
            finally:
                self.commands.task_done()


def make_handler(dispatcher: CommandDispatcher, token: str) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "torrent-client-management/1"

        def log_message(self, fmt: str, *args: Any) -> None:
            # The standard request line contains no authorization header.
            super().log_message(fmt, *args)

        def _send(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _authorized(self) -> bool:
            supplied = self.headers.get("Authorization", "")
            return hmac.compare_digest(supplied, f"Bearer {token}")

        def _route(self, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
            path = urllib.parse.urlsplit(self.path).path
            parts = [urllib.parse.unquote(part) for part in path.split("/") if part]
            if parts[:2] != ["api", "v1"]:
                raise ManagementError("not_found", "endpoint not found", 404)
            tail = parts[2:]
            if self.command == "GET" and tail == ["status"]:
                return 200, dispatcher.invoke("status")
            if self.command == "GET" and tail == ["torrents"]:
                return 200, dispatcher.invoke("list_torrents")
            if self.command == "GET" and len(tail) == 2 and tail[0] == "torrents":
                return 200, dispatcher.invoke("get_torrent", tail[1])
            if self.command == "POST" and tail == ["torrents"]:
                return 202, dispatcher.invoke("add_torrent", (body or {}).get("source"))
            if self.command == "POST" and len(tail) == 3 and tail[0] == "torrents":
                return 200, dispatcher.invoke("action", tail[1], tail[2])
            raise ManagementError("not_found", "endpoint not found", 404)

        def _handle(self) -> None:
            if not self._authorized():
                self._send(401, ManagementError("unauthorized", "valid bearer token required", 401).payload())
                return
            try:
                body = None
                if self.command == "POST":
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                    except ValueError as exc:
                        raise ManagementError("invalid_json", "invalid Content-Length") from exc
                    if length > 64 * 1024:
                        raise ManagementError("request_too_large", "request body exceeds 64 KiB", 413)
                    try:
                        body = json.loads(self.rfile.read(length) or b"{}")
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        raise ManagementError("invalid_json", "request body must be JSON") from exc
                    if not isinstance(body, dict):
                        raise ManagementError("invalid_json", "request body must be a JSON object")
                status, payload = self._route(body)
                self._send(status, payload)
            except ManagementError as exc:
                self._send(exc.status, exc.payload())
            except Exception:
                self._send(500, ManagementError("internal_error", "internal server error", 500).payload())

        do_GET = _handle
        do_POST = _handle

    return Handler


def start_management_server(
    host: str, port: int, token: str, allow_remote: bool, dispatcher: CommandDispatcher
) -> ThreadingHTTPServer | None:
    validate_management_config(host, port, token, allow_remote)
    if port == 0:
        return None
    server = ThreadingHTTPServer((host, port), make_handler(dispatcher, token))
    thread = threading.Thread(target=server.serve_forever, name="management-http", daemon=True)
    thread.start()
    return server


class APIClient:
    def __init__(self, address: str, token: str, timeout: float = 10.0):
        self.address = address.rstrip("/")
        self.token = token
        self.timeout = timeout

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            f"{self.address}/api/v1{path}",
            data=data,
            method=method,
            headers={"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            try:
                payload = json.load(exc)
                error = payload["error"]
                raise ManagementError(error["code"], error["message"], exc.code) from exc
            except (KeyError, json.JSONDecodeError):
                raise ManagementError("http_error", f"server returned HTTP {exc.code}", exc.code) from exc
        except urllib.error.URLError as exc:
            raise ManagementError("connection_error", f"cannot reach management API: {exc.reason}", 0) from exc
