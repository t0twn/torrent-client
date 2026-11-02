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

Management HTTP is disabled by default (`MANAGEMENT_PORT=0`). Every enabled listener requires a bearer token. The safe local-host setup is:

```console
export MANAGEMENT_TOKEN='replace-with-a-long-random-secret'
export MANAGEMENT_PORT=8080
export MANAGEMENT_INSTANCE_ID='seedbox-east'
uv run torrent-client
```

The default management bind is `127.0.0.1`; this remains private even though `docker-compose.yml` uses host networking. `MANAGEMENT_INSTANCE_ID` defaults to the hostname and appears in every successful API response so an external aggregator can distinguish instances.

Enabled listeners allow at most 16 active HTTP connections. A request has three seconds to complete its headers and five seconds to complete its POST body; incomplete requests are disconnected, and excess connections are closed. These are absolute deadlines, including clients that send data slowly in small pieces.

A non-loopback bind is rejected unless `--management-allow-remote` is supplied. For access beyond the host, put the loopback listener behind an authenticated TLS reverse proxy whenever possible. Bearer tokens sent over plain HTTP can be intercepted. Do not reuse the public BitTorrent port or publish the management port directly to the internet.

Supported environment settings are:

| Setting | Default | Meaning |
| --- | --- | --- |
| `MANAGEMENT_HOST` | `127.0.0.1` | HTTP bind address |
| `MANAGEMENT_PORT` | `0` | HTTP port; zero disables management |
| `MANAGEMENT_TOKEN` | empty | Required bearer secret when enabled |
| `MANAGEMENT_INSTANCE_ID` | hostname | Stable operator-selected instance identity |

## CLI

`torrentctl` always targets an explicit origin and token, either as flags or environment variables:

```console
export TORRENT_CLIENT_ADDRESS=http://127.0.0.1:8080
export TORRENT_CLIENT_TOKEN="$MANAGEMENT_TOKEN"

uv run torrentctl status
uv run torrentctl list
uv run torrentctl get 0123456789abcdef0123456789abcdef01234567
uv run torrentctl add 'magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567'
uv run torrentctl pause 0123456789abcdef0123456789abcdef01234567
uv run torrentctl resume all
uv run torrentctl reannounce all
uv run torrentctl --json list
```

`all` means every currently valid handle in this process. It is accepted only for pause, resume, and reannounce. Quit remains a local interactive `q` command.

CLI exit codes are stable: `0` success, `2` command-line/configuration error, `3` API error, and `4` connection failure. Human output goes to stdout, JSON output is selected with `--json`, and errors go to stderr. Avoid passing a token directly on shared machines where process arguments are visible; prefer `TORRENT_CLIENT_TOKEN`.

## HTTP API v1

All requests require `Authorization: Bearer <token>` and all bodies are JSON.

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
