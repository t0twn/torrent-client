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

## Sniff-mode scheduler

When running in sniff mode (the default, no command-line torrents), the process listens to DHT `announce_peer` messages and opportunistically fetches metadata to cache as `.torrent` files. A bounded scheduler sits between the announce alert and `session.async_add_torrent` so a burst of unique hashes cannot saturate the libtorrent session or the metadata persistence pipeline.

The scheduler is disabled in download mode. The `.torrent` cache, `TORRENTS` TTL, `LIVE_HANDLES`, and the session upload/download rate flags all continue to work unchanged.

Supported scheduler environment settings:

| Setting | Default | Meaning |
| --- | --- | --- |
| `SNIFF_MAX_QUEUE` | `64` | Maximum candidates waiting for an in-flight slot |
| `SNIFF_MAX_IN_FLIGHT` | `4` | Maximum concurrent metadata acquisitions |
| `SNIFF_MAX_STARTS_PER_SECOND` | `1.0` | Maximum new acquisitions started per rate window |
| `SNIFF_START_RATE_WINDOW` | `1.0` | Rate window in seconds |
| `SNIFF_ACQUISITION_TIMEOUT` | `120.0` | Seconds before an in-flight acquisition is timed out |
| `SNIFF_QUEUE_WAIT_TIMEOUT` | `300.0` | Seconds a candidate may wait in queue before cooldown |
| `SNIFF_PERSIST_TIMEOUT` | `600.0` | Seconds a PERSISTING candidate waits before recovery timeout |
| `SNIFF_COOLDOWN_BASE` | `30.0` | Initial retry delay after failure (seconds) |
| `SNIFF_COOLDOWN_MAX` | `3600.0` | Maximum retry delay (seconds) |
| `SNIFF_COOLDOWN_MULTIPLIER` | `2.0` | Exponential backoff multiplier |
| `SNIFF_COOLDOWN_JITTER` | `0.25` | ±fraction of random jitter applied to each delay |
| `SNIFF_MAX_COOLDOWNS` | `512` | Maximum distinct hashes tracked in cooldown registry |
| `SNIFF_COOLDOWN_TTL` | `7200.0` | Seconds before a cooldown entry expires |
| `SNIFF_DIAGNOSTICS_INTERVAL` | `60.0` | Seconds between aggregate log lines |
| `SNIFF_DISTINCT_HASH_BUDGET` | `256` | Distinct hash tracking budget per diagnostics window |
| `SNIFF_MAX_CANDIDATES` | `2048` | Total cap on scheduler candidate records (queued + in-flight + cooldown) |
| `SNIFF_QUEUE_DROP_COOLDOWN` | `30.0` | Seconds of bounded overload suppression after a queue-full drop |

All values must be positive. Invalid values fail fast at startup with a clear error message.

### Diagnostics

Every `SNIFF_DIAGNOSTICS_INTERVAL` seconds, the alert loop logs a single aggregate line prefixed with `sniff-scheduler`. Each field is a per-window counter:

```
sniff-scheduler announce_peer_received=142 announce_peer_invalid=0
  distinct_hashes=97 hash_set_saturated=false hash_overflow=0
  skip_cached_file=18 skip_pending=0 skip_in_flight=12 skip_persisting=3
  skip_cooldown=7 skip_known_to_session=5
  drop_queue_full=0 admitted_queued=0 admitted_started=97
  add_started=97 add_failed_sync=0 add_failed_async=1 invalid_handle_alert=1
  metadata_success=95 metadata_missing=0 metadata_timeout=1 late_alert_ignored=0
  persist_success=94 persist_failure=1 persist_queue_full=0
  queue_wait_timeouts=0 queue_evictions=0
  cooldown_entries_recorded=3 cooldown_entries_evicted=0
  queued=0 in_flight=3 persisting=1 cooldowns=2
```

Info-hashes are never included in the periodic `sniff-scheduler` diagnostic line or used as metric labels. All scheduler logging in the sniff path is hash-agnostic. Other operational log lines in the process (e.g. `persist_torrent_metadata` success/failure, resume data failures) are also hash-free in sniff mode; any per-hash log lines come from non-sniff paths (download-mode resume data, management API) and are not emitted during pure sniff-mode operation.

`announce_peer_received` counts valid DHT `announce_peer` messages after decode and info-hash extraction. `announce_peer_invalid` counts only packets that failed decode or whose info-hash field was missing/unparsable; well-formed DHT queries that are not `announce_peer` are silently ignored (they are normal DHT traffic, not announces). The scheduler's skip / drop counters cover every non-admitted announce; the sum of all skip, drop, and admitted counters equals `announce_peer_received` minus `announce_peer_invalid` minus `skip_cached_file`.

### Baseline procedure

These counters are the supported way to attribute sniff-mode load. To measure a baseline:

1. Start the process in sniff mode with the default configuration.
2. Let it run for at least `SNIFF_DIAGNOSTICS_INTERVAL` seconds.
3. Collect consecutive `sniff-scheduler` log lines.
4. Compare `admitted_started` (new metadata acquisitions the scheduler launched) against the host or container RX/TX bytes from `ss`, `ip -s link`, or cgroup counters over the same window. Do **not** treat `announce_peer_received` as network bytes; it counts DHT query messages observed by libtorrent's alert path, not total NIC traffic. The scheduler does not measure DHT byte volume; DHT and metadata/peer traffic are distinct. `announce_peer_received` is a message count; `admitted_started` is a metadata-acquisition count; neither is the host's total byte count.
5. If a specific host/container network cap is required, enforce it at the deployment layer (cgroup, iptables, orchestrator limit). The scheduler does not measure or enforce total NIC bytes.

Report any measured baseline only if actually measured; otherwise document that the production cause of high traffic remains unproven and reproduce with the diagnostic counters above. **No production traffic measurement was performed as part of this PR**; the counters and baseline procedure are the mechanism by which an operator can measure their own deployment.

### Throughput tradeoffs

- Lower `SNIFF_MAX_IN_FLIGHT` reduces concurrent metadata traffic but increases queue wait time and the chance of `queue_wait_timeouts`.
- Higher `SNIFF_MAX_STARTS_PER_SECOND` admits bursts at the cost of more simultaneous metadata connections.
- Shorter `SNIFF_ACQUISITION_TIMEOUT` frees slots faster but may abort acquisitions that would have succeeded on a slower network.
- Overload drops (queue full) apply a short bounded cooldown (`SNIFF_QUEUE_DROP_COOLDOWN`, default 30 s) so the same hash does not immediately re-announce and hit the full queue again. This is deliberately shorter than the acquisition-failure cooldown; repeated drops for the same hash compound via the normal backoff curve on subsequent failures.

### Restart and cache cleanup

The scheduler's queue, in-flight set, and cooldown registry are in-memory only. A process restart clears them. The authoritative durable signal is the `.torrent` cache file in `TORRENT_DIR`: any hash whose cache file exists is skipped before the scheduler is consulted, even across restarts. Daily cache cleanup (`cleanup_old_cache_files`, default 7 days) removes files older than the configured threshold; hashes whose cache was deleted become eligible for reacquisition, subject to the usual scheduler bounds.

### Hourly / daily budget policy

The scheduler does not enforce a byte budget itself; the counters above provide the inputs an operator needs to enforce one at the deployment layer. A concrete policy proposal for a future extension:

- **Measurement point.** Use host / container network counters: `ip -s link` on the host NIC, or the orchestrator's pod/container network accounting (Kubernetes `container_network_receive_bytes_total` / `container_network_transmit_bytes_total` via cAdvisor, or the cloud provider's per-instance network metrics). cgroup v2 does not currently expose per-cgroup network byte counters — `io.stat` measures block I/O and `memory.current` measures memory, neither of which is network traffic. Do not derive bytes from `announce_peer_received` or `admitted_started`.
- **Window.** Rolling 1-hour and 24-hour windows evaluated in the alert loop's diagnostics tick. The scheduler records `admitted_started` per window; the deployment layer records bytes per window.
- **Stopping new starts.** When the rolling window exceeds the configured budget, a future flag could set `SNIFF_MAX_STARTS_PER_SECOND=0` and let in-flight acquisitions finish naturally. In-flight tasks at that point are **not** aborted: they complete normally and their results are still persisted.
- **Restart continuity.** Budget counters are not durable. After restart, the window resets. The `.torrent` cache continues to suppress reacquisition of cached hashes regardless of budget.
- **Overage recording.** Each window that exceeds its budget emits a one-line `sniff-budget` log with the measured value, the limit, and the window length. No automatic enforcement occurs in this release.
- **No production baseline.** This policy is unvalidated against production traffic. Operators should measure with the `sniff-scheduler` counters and host RX/TX counters for at least 24 hours before setting limits.

This is explicitly a future extension; the current release provides only the observation surface. A strict host/container network cap needs deployment-level counters and enforcement.

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
