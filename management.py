"""Versioned management service, HTTP transport, and API client."""

from __future__ import annotations

import base64
import contextlib
import errno
import hmac
import ipaddress
import json
import queue
import socket
import socketserver
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import Future
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler
from typing import Any, Callable


class ManagementError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def payload(self) -> dict[str, Any]:
        return {"error": {"code": self.code, "message": self.message}}


class LiveHandles:
    """Live libtorrent handles, independent of the expiring deduplication cache.

    All mutations and reads occur on the alert-loop thread.
    """

    def __init__(self, maxsize: int):
        self.maxsize = maxsize
        self._handles: dict[str, Any] = {}

    def put(self, info_hash: str, handle: Any) -> bool:
        self.items()
        if info_hash not in self._handles and len(self._handles) >= self.maxsize:
            return False
        self._handles[info_hash] = handle
        return True

    def discard(self, info_hash: str) -> None:
        self._handles.pop(info_hash, None)

    def get(self, info_hash: str) -> Any:
        handle = self._handles.get(info_hash)
        if handle is not None and not handle.is_valid():
            self.discard(info_hash)
            return None
        return handle

    def items(self) -> list[tuple[str, Any]]:
        for info_hash in list(self._handles):
            self.get(info_hash)
        return list(self._handles.items())

    def __contains__(self, info_hash: str) -> bool:
        return self.get(info_hash) is not None

    def __len__(self) -> int:
        self.items()
        return len(self._handles)


def _parse_host_value(host_value: str) -> str:
    """Strip an optional port / bracket from a Host-style value, returning the bare host."""
    value = (host_value or "").strip()
    if not value:
        return ""
    if value.startswith("["):
        # IPv6 with brackets: [::1] or [::1]:port
        close = value.find("]")
        if close < 0:
            return value.lstrip("[")
        return value[1:close]
    if ":" in value:
        # Either host:port or a bare IPv6 address; IPv6 literal always has multiple colons.
        if value.count(":") == 1:
            return value.rsplit(":", 1)[0]
        return value
    return value


def _is_loopback_host(host_value: str) -> bool:
    """Return True when `host_value` is any loopback identifier (127/8, localhost, ::1)."""
    host = _parse_host_value(host_value)
    if not host:
        return False
    normalized = host.lower()
    if normalized == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def peer_is_loopback(client_address: tuple) -> bool:
    """True when the TCP peer address is IPv4/IPv6 loopback (incl. IPv4-mapped IPv6)."""
    if not client_address:
        return False
    host = client_address[0]
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(addr, ipaddress.IPv6Address):
        mapped = addr.ipv4_mapped
        if mapped is not None:
            return mapped.is_loopback
    return addr.is_loopback


def validate_management_config(host: str, port: int, token: str, allow_remote: bool) -> None:
    if port == 0:
        return
    if not 1 <= port <= 65535:
        raise ValueError("management port must be between 1 and 65535, or 0 to disable")
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ValueError(f"invalid management bind address: {host}") from exc
    is_loopback = all(
        ipaddress.ip_address(info[4][0].split("%")[0]).is_loopback for info in infos
    )
    if is_loopback:
        # Loopback listeners are reachable only from the local host. The token is
        # optional because peer-loopback requests are exempt from bearer auth.
        return
    if not allow_remote:
        raise ValueError(
            "non-loopback management bind requires --management-allow-remote"
        )
    if not token:
        raise ValueError(
            "non-loopback management bind requires a non-empty MANAGEMENT_TOKEN"
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
            "invalid_source", "v1 accepts only magnet URIs or 40/64-character info hashes"
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
        pending_registry: Any = None,
    ):
        self.session = session
        self.registry = registry
        self.pending_registry = pending_registry
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
        if info_hash in self.registry or (
            self.pending_registry is not None and info_hash in self.pending_registry
        ):
            raise ManagementError("duplicate", "torrent is already registered", 409)
        if len(self.registry) >= self.registry.maxsize or (
            self.pending_registry is not None and len(self.pending_registry) >= self.pending_registry.maxsize
        ):
            raise ManagementError("capacity_reached", "live torrent capacity reached", 409)
        try:
            accepted = self.add_callback(normalized)
        except Exception as exc:
            raise ManagementError("add_failed", "libtorrent rejected source", 422) from exc
        if not accepted:
            # A concurrent alert can reserve the same hash between checks.
            if info_hash in self.registry or (
                self.pending_registry is not None and info_hash in self.pending_registry
            ):
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

    def __init__(self, service: Any, max_pending: int = 128):
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
            if future.done():
                raise
            if future.cancel():
                raise ManagementError(
                    "command_cancelled", "management command timed out before execution", 503
                ) from exc
            raise ManagementError(
                "outcome_unknown", "management command timed out after execution began; query state before retrying", 504
            ) from exc

    def drain(self, max_commands: int = 32) -> None:
        for _ in range(max_commands):
            try:
                command = self.commands.get_nowait()
            except queue.Empty:
                return
            try:
                if not command.future.set_running_or_notify_cancel():
                    continue
                command.future.set_result(getattr(self.service, command.method)(*command.args))
            except Exception as exc:
                command.future.set_exception(exc)
            finally:
                self.commands.task_done()


class BoundedHTTPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    """Reject surplus clients before a worker thread is allocated.

    Automatically selects AF_INET or AF_INET6 based on the bind address so
    both IPv4 (e.g. 127.0.0.1) and IPv6 (e.g. ::1) binds work.
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self, address: tuple[str, int], handler: type[BaseHTTPRequestHandler],
        *, max_connections: int = 16, header_timeout: float = 3.0,
        body_timeout: float = 5.0,
    ):
        self._slots = threading.BoundedSemaphore(max_connections)
        self.header_timeout = header_timeout
        self.body_timeout = body_timeout
        self.address_family = _address_family_for(address[0])
        super().__init__(address, handler)

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()


def _address_family_for(host: str) -> int:
    """Return AF_INET6 when `host` resolves to an IPv6 address, else AF_INET."""
    bare = host.split("%")[0] if "%" in host else host
    try:
        addr = ipaddress.ip_address(bare)
    except ValueError:
        try:
            infos = socket.getaddrinfo(bare, None, type=socket.SOCK_STREAM)
        except socket.gaierror:
            return socket.AF_INET
        if infos and infos[0][0] == socket.AF_INET6:
            return socket.AF_INET6
        return socket.AF_INET
    if isinstance(addr, ipaddress.IPv6Address):
        return socket.AF_INET6
    return socket.AF_INET


def _is_loopback_literal(host: str) -> bool:
    """True if `host` is a loopback IP literal (127/8, ::1) or 'localhost'."""
    host = host.strip().lower()
    if not host:
        return False
    if host == "localhost":
        return True
    # Strip IPv6 brackets if present
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _normalize_host_for_comparison(host: str) -> str:
    """Normalize a host for exact string comparison.

    Lowercases and strips IPv6 brackets. Two hosts that normalize to the
    same string are considered the same origin for the same-origin check.
    This does NOT equate different loopback IPs (e.g. 127.0.0.1 vs 127.0.0.2).
    """
    host = (host or "").strip().lower()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    return host


def _parse_origin(origin: str) -> tuple[str, str, int] | None:
    """Parse an Origin header into (scheme, host, port).

    Returns None when the Origin is malformed, has an unsupported scheme,
    contains a path or userinfo, or has an invalid port. Any parser error
    is caught and treated as malformed — callers never see an exception.
    """
    if not origin:
        return None
    try:
        parsed = urllib.parse.urlsplit(origin)
    except ValueError:
        return None
    scheme = (parsed.scheme or "").lower()
    if scheme not in ("http", "https"):
        return None
    # Reject Origins with userinfo or path components — a valid origin is
    # strictly scheme://host[:port] with nothing else.
    if parsed.username or parsed.password:
        return None
    if parsed.path and parsed.path not in ("", "/"):
        return None
    if parsed.query or parsed.fragment:
        return None
    try:
        host = (parsed.hostname or "").lower()
    except ValueError:
        return None
    if not host:
        return None
    try:
        port = parsed.port
    except ValueError:
        return None
    if port is None:
        port = 443 if scheme == "https" else 80
    return scheme, host, port


def _parse_host_port(host_value: str) -> tuple[str, int | None, bool]:
    """Parse a Host header into (host, port, well_formed).

    `well_formed` is False when the value cannot be parsed cleanly.
    Port is None when no port is present.
    """
    value = (host_value or "").strip()
    if not value:
        return "", None, False
    if value.startswith("["):
        close = value.find("]")
        if close < 0:
            return value.lstrip("["), None, False
        host = value[1:close]
        rest = value[close + 1:]
        if not rest:
            return host, None, True
        if rest == ":":
            return host, None, False
        if rest.startswith(":"):
            try:
                return host, int(rest[1:]), True
            except ValueError:
                return host, None, False
        return host, None, False
    if value.count(":") == 1:
        host_part, port_part = value.rsplit(":", 1)
        try:
            return host_part, int(port_part), True
        except ValueError:
            return "", None, False
    if ":" in value:
        # Multiple colons with no brackets → bare IPv6, no port
        return value, None, True
    return value, None, True


def _origin_is_loopback(origin: str) -> bool:
    """True when the Origin header value identifies a loopback endpoint.

    Kept for backward compatibility with unit-level checks. The HTTP handler
    uses `_origin_matches_target` for strict validation instead.
    """
    parsed = _parse_origin(origin)
    if parsed is None:
        return False
    _, host, _ = parsed
    return _is_loopback_literal(host)


def make_handler(dispatcher: CommandDispatcher, token: str) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "torrent-client-management/1"

        def handle_one_request(self) -> None:
            try:
                super().handle_one_request()
            except OSError:
                # The deadline may close the socket while the worker is reading
                # or writing. The connection is already gone; release its slot.
                self.close_connection = True

        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(self.server.header_timeout)
            self._header_timer = threading.Timer(self.server.header_timeout, self._expire)
            self._header_timer.daemon = True
            self._header_timer.start()
            self._body_timer = None

        def _expire(self) -> None:
            with contextlib.suppress(OSError):
                self.connection.shutdown(socket.SHUT_RDWR)

        def parse_request(self) -> bool:
            try:
                return super().parse_request()
            finally:
                self._header_timer.cancel()
                if getattr(self, "command", None) == "POST":
                    self.connection.settimeout(self.server.body_timeout)
                    self._body_timer = threading.Timer(self.server.body_timeout, self._expire)
                    self._body_timer.daemon = True
                    self._body_timer.start()

        def finish(self) -> None:
            self._header_timer.cancel()
            if self._body_timer is not None:
                self._body_timer.cancel()
            super().finish()

        def log_message(self, fmt: str, *args: Any) -> None:
            # Request targets are untrusted and can contain credentials.
            return

        def _send(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _peer_is_loopback(self) -> bool:
            return peer_is_loopback(getattr(self, "client_address", None))

        def _origin_host_is_safe(self) -> bool:
            """For loopback-bound listeners: reject cross-site browser requests.

            When the request carries an Origin header (browsers on cross-site
            requests and on POST), the Origin must exactly match the target
            origin under a defined normalization — same scheme (always http
            for this plain-HTTP server), identical port, and the Origin host
            must normalize to the same string as the validated request Host
            host. Different loopback IPs (e.g. 127.0.0.1 vs 127.0.0.2) are
            NOT considered the same origin even though both are loopback.

            The comparison uses the validated request Host, not the bind
            address, so a valid same-origin browser request on a wildcard
            listener (0.0.0.0 / ::) works as long as the Host and Origin
            identify the same loopback endpoint.

            When no Origin is present (native CLI shape), the Host header
            must still identify a loopback endpoint with a well-formed port
            that matches the bound port. This defends against DNS rebinding
            and Host-forging attacks.

            Forwarding headers (X-Forwarded-For, Forwarded, X-Real-IP, ...)
            are intentionally ignored — a reverse proxy that delivers external
            requests as loopback is the proxy's responsibility to police.
            """
            # Parse the Host header first — required and must be well-formed.
            raw_host = self.headers.get("Host", "")
            host, port, well_formed = _parse_host_port(raw_host)
            if not well_formed or not host:
                return False
            if not _is_loopback_host(host):
                return False
            # If the Host carries a port, it must match the bound port.
            server_port = self.server.server_address[1]
            if port is not None and port != server_port:
                return False
            # If no port in Host, the default is 80; reject if server isn't on 80.
            if port is None and server_port != 80:
                return False

            # If an Origin header is present, it must match the target exactly.
            origin = self.headers.get("Origin")
            if origin is not None:
                parsed = _parse_origin(origin)
                if parsed is None:
                    # Malformed Origin (null, bad scheme, path, userinfo, bad port)
                    return False
                scheme, origin_host, origin_port = parsed
                # Scheme must be http (this is a plain-HTTP server)
                if scheme != "http":
                    return False
                # Port must match exactly
                if origin_port != server_port:
                    return False
                # Origin host must match the validated request Host host exactly
                # under normalization — NOT loopback-equivalence. Different
                # loopback IPs (127.0.0.1 vs 127.0.0.2) are different origins.
                normalized_origin = _normalize_host_for_comparison(origin_host)
                normalized_host = _normalize_host_for_comparison(host)
                if normalized_origin != normalized_host:
                    return False
            return True

        def _authorized(self) -> tuple[bool, str]:
            """Return (authorized, rejection_reason).

            Authorization is based solely on the TCP peer address. Loopback
            peers are exempt from bearer auth (a configured token is still
            accepted, but not required). Non-loopback peers must present the
            configured bearer token. Host, Origin, X-Forwarded-For, and any
            other request header are never used to infer local access.
            """
            if self._peer_is_loopback():
                return True, ""
            if not token:
                return False, "non-loopback access requires a configured token"
            supplied = self.headers.get("Authorization", "")
            if not hmac.compare_digest(supplied, f"Bearer {token}"):
                return False, "valid bearer token required"
            return True, ""

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
            # For loopback peers, apply the same-origin / Host safeguard before
            # granting the token exemption. Non-loopback peers do not need it
            # because bearer auth is enforced for them.
            if self._peer_is_loopback() and not self._origin_host_is_safe():
                self._send(
                    403,
                    ManagementError(
                        "forbidden_origin",
                        "request rejected by same-origin safeguard",
                        403,
                    ).payload(),
                )
                return
            authorized, reason = self._authorized()
            if not authorized:
                self._send(
                    401,
                    ManagementError("unauthorized", reason or "unauthorized", 401).payload(),
                )
                return
            try:
                body = None
                if self.command == "POST":
                    try:
                        length = int(self.headers.get("Content-Length", "0"))
                    except ValueError as exc:
                        raise ManagementError("invalid_json", "invalid Content-Length") from exc
                    if length < 0:
                        raise ManagementError("invalid_json", "invalid Content-Length")
                    if length > 64 * 1024:
                        raise ManagementError("request_too_large", "request body exceeds 64 KiB", 413)
                    try:
                        body = json.loads(self.rfile.read(length) or b"{}")
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        raise ManagementError("invalid_json", "request body must be JSON") from exc
                    finally:
                        if self._body_timer is not None:
                            self._body_timer.cancel()
                    if not isinstance(body, dict):
                        raise ManagementError("invalid_json", "request body must be a JSON object")
                status, payload = self._route(body)
                self._send(status, payload)
            except ManagementError as exc:
                self._send(exc.status, exc.payload())
            except OSError:
                return
            except Exception:
                self._send(500, ManagementError("internal_error", "internal server error", 500).payload())

        do_GET = _handle
        do_POST = _handle

    return Handler


def start_management_server(
    host: str, port: int, token: str, allow_remote: bool, dispatcher: CommandDispatcher
) -> BoundedHTTPServer | None:
    validate_management_config(host, port, token, allow_remote)
    if port == 0:
        return None
    try:
        server = BoundedHTTPServer((host, port), make_handler(dispatcher, token))
    except OSError as exc:
        # Preserve the underlying cause. Only mention port conflict when the
        # kernel actually reported EADDRINUSE; other failures (unsupported
        # address family, missing IPv6 stack, permission denied, …) surface
        # with their real message.
        if exc.errno == errno.EADDRINUSE:
            raise OSError(
                f"failed to bind management HTTP on {host}:{port}; "
                "choose a different management port or stop the conflicting listener"
            ) from exc
        raise OSError(
            f"failed to bind management HTTP on {host}:{port}: {exc.strerror or exc}"
        ) from exc
    thread = threading.Thread(target=server.serve_forever, name="management-http", daemon=True)
    thread.start()
    return server


class APIClient:
    def __init__(self, address: str, token: str, timeout: float = 10.0):
        self.address = address.rstrip("/")
        # A missing token is legitimate on loopback listeners. We deliberately
        # do not fabricate a fake bearer credential in that case.
        self.token = (token or "").strip()
        self.timeout = timeout

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        data = json.dumps(body).encode() if body is not None else None
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        request = urllib.request.Request(
            f"{self.address}/api/v1{path}",
            data=data,
            method=method,
            headers=headers,
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
