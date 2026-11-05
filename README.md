# torrent-client

A small libtorrent client that can run in metadata-sniffing mode or download mode. It includes an optional, versioned management API and a separate-process `torrentctl` client for one running instance.

## Run

Install the declared dependencies and view the compatible startup flags:

```console
uv sync
uv run torrent-client --help
```

Existing startup torrent arguments remain supported and switch the process into download mode:

```console
uv run torrent-client --save-path ./downloads 'magnet:?xt=urn:btih:...'
```

The BitTorrent peer listener is configured by `--listen-interface` and `--port`. It is separate from the HTTP management listener described below.

## Enable management HTTP

The management HTTP listener is enabled on loopback by default: `127.0.0.1:8080`. A single instance started with no configuration is immediately usable from the local CLI (`torrentctl status`, `torrentctl list`, ...) and from any other local process.

To disable the listener entirely, set the port to `0`:

```console
export MANAGEMENT_PORT=0
uv run torrent-client
```

For multiple instances on the same host, pick a different management port for each one so the sockets do not collide — a port conflict fails loudly with a hint to choose another management port, it never silently falls back:

```console
# instance A
MANAGEMENT_PORT=8080 uv run torrent-client

# instance B
MANAGEMENT_PORT=8081 uv run torrent-client
```

The default bind is `127.0.0.1`; this is a loopback address that is only reachable from the host itself (including other processes on the host, and containers that share its network namespace via `network_mode: host`). The BitTorrent peer listener (`--listen-interface` / `--port`) is unrelated — it accepts remote peers from the internet; the management listener does not.

Remote (non-loopback) binds are rejected unless both `--management-allow-remote` and a non-empty `MANAGEMENT_TOKEN` are supplied. The safe way to expose management beyond the host is to leave the loopback listener in place and put an authenticated TLS reverse proxy in front of it. Bearer tokens sent over plain HTTP can be intercepted. Do not reuse the public BitTorrent port or publish the management port directly to the internet.

`MANAGEMENT_INSTANCE_ID` defaults to the hostname and appears in every successful API response so an external aggregator can distinguish instances.

Enabled listeners allow at most 16 active HTTP connections. A request has three seconds to complete its headers and five seconds to complete its POST body; incomplete requests are disconnected, and excess connections are closed. These are absolute deadlines, including clients that send data slowly in small pieces.

Supported environment settings are:

| Setting | Default | Meaning |
| --- | --- | --- |
| `MANAGEMENT_HOST` | `127.0.0.1` | HTTP bind address; loopback recommended |
| `MANAGEMENT_PORT` | `8080` | HTTP port; `0` disables management |
| `MANAGEMENT_TOKEN` | empty | Bearer secret, optional on loopback, required for non-loopback |
| `MANAGEMENT_INSTANCE_ID` | hostname | Stable operator-selected instance identity |

## CLI

`torrentctl` defaults to `http://127.0.0.1:8080`. The address resolution order is:

1. `--address <origin>`
2. `TORRENT_CLIENT_ADDRESS`
3. `http://127.0.0.1:8080`

The token is optional for the default loopback listener and is resolved in the same way:

1. `--token <value>`
2. `TORRENT_CLIENT_TOKEN`

```console
# default — works against a stock instance with no configuration
uv run torrentctl status
uv run torrentctl list
uv run torrentctl add 'magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567'

# explicit remote endpoint
uv run torrentctl --address http://seedbox.example:8080 --token "$TOKEN" list
```

`all` means every currently valid handle in this process. It is accepted only for pause, resume, and reannounce. Quit remains a local interactive `q` command.

CLI exit codes are stable: `0` success, `2` command-line/configuration error, `3` API error, and `4` connection failure. Human output goes to stdout, JSON output is selected with `--json`, and errors go to stderr. Avoid passing a token directly on shared machines where process arguments are visible; prefer `TORRENT_CLIENT_TOKEN`.

## Security boundaries

Authorization is decided from the actual TCP peer address — the `client_address` of the accepted socket — not from request headers. Loopback peers (IPv4 `127.0.0.0/8`, IPv6 `::1`, and IPv4-mapped IPv6 loopback such as `::ffff:127.0.0.1`) are exempt from bearer authentication regardless of whether `MANAGEMENT_TOKEN` is configured. Non-loopback peers always need the correct bearer token; a missing or incorrect token is rejected with HTTP `401`.

Loopback listeners also enforce a same-origin / Host safeguard against cross-site browser requests and DNS-rebinding attacks: requests whose `Origin` header (when present) or `Host` header identifies a non-loopback endpoint are rejected with HTTP `403`. Native CLI requests (which do not send `Origin`) continue to work unchanged. Forwarding headers such as `X-Forwarded-For`, `Forwarded`, or `X-Real-IP` are deliberately not consulted — when a reverse proxy or tunnel delivers external requests as loopback connections, the proxy is responsible for policing its own access. This safeguard identifies the TCP peer; it does not identify the original client behind a proxy, and claims otherwise should be treated skeptically.

`docker-compose.yml` uses host networking by default, which means the loopback management listener is reachable from the host and from other containers that also use the host network namespace. It is not reachable from bridged containers. Do not re-publish the management port to the internet.


## HTTP API v1

Loopback requests do not require authentication. Non-loopback requests must include `Authorization: Bearer <token>` and are rejected with HTTP `401` when the token is missing or incorrect. All bodies are JSON.

| Method and path | Result |
| --- | --- |
| `GET /api/v1/status` | Instance identity, mode, live count, capacity |
| `GET /api/v1/torrents` | Snapshots of live libtorrent handles |
| `GET /api/v1/torrents/{info_hash}` | One live-handle snapshot |
| `POST /api/v1/torrents` | Asynchronously submit `{"source":"<magnet-or-v1-hash>"}` |
| `POST /api/v1/torrents/{info_hash-or-all}/pause` | Pause target(s) |
| `POST /api/v1/torrents/{info_hash-or-all}/resume` | Resume target(s) |
| `POST /api/v1/torrents/{info_hash-or-all}/reannounce` | Force tracker reannounce |

Example:

```console
curl -H "Authorization: Bearer $MANAGEMENT_TOKEN" \
  http://127.0.0.1:8080/api/v1/status

curl -X POST -H "Authorization: Bearer $MANAGEMENT_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"source":"0123456789abcdef0123456789abcdef01234567"}' \
  http://127.0.0.1:8080/api/v1/torrents
```

Successful status response:

```json
{
  "instance_id": "seedbox-east",
  "mode": "download",
  "live_torrent_count": 1,
  "capacity": 100
}
```

Each live torrent has these fields:

- `info_hash`: lowercase 40-character v1 or 64-character v2 info hash, the stable resource identifier
- `name`, numeric libtorrent `state`, `paused`, and `has_metadata`
- `progress`: fraction from `0.0` through `1.0`
- `total_size_bytes`, `total_download_bytes`, and `total_upload_bytes`
- `download_rate_bytes_per_second` and `upload_rate_bytes_per_second`
- `num_peers` and `num_seeds`

The live endpoints reflect the in-process handle registry only. They do not report SQLite metadata rows as active torrents. The metadata storage contract remains unchanged: `Torrent.total_size` is bytes and `TorrentFile.file_size` is whole MiB.

The live-handle inventory is independent of the expiring TTL cache used for discovery deduplication. A valid torrent stays visible and actionable after the deduplication entry expires; removal from the session removes it from the live inventory.

Adding returns HTTP `202` with `status: "accepted"`; libtorrent completes the asynchronous add later. V1 accepts a magnet containing a v1 `btih` or SHA-256 `btmh` identifier, or a 40-character v1 / 64-character v2 info hash. It never accepts a server-side `.torrent` path or file upload. Duplicate and capacity conflicts return HTTP `409`.

Errors have one shape:

```json
{"error":{"code":"not_found","message":"live torrent not found"}}
```

Common codes are `unauthorized` (401), `invalid_json` or `invalid_source` (400), `not_found` (404), `duplicate` or `capacity_reached` (409), `add_failed` (422), and `service_unavailable` (503). A queued command that times out before execution returns `command_cancelled` (503) and will not run later. If execution has already begun, the response is `outcome_unknown` (504); query the torrent state before retrying a mutation. Unknown internal failures return a generic `internal_error` without exposing file paths or exception details.

## Tests

```console
uv run python -m unittest discover -s tests -v
```
