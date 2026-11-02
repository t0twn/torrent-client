#!/usr/bin/env python3
"""Separate-process command line client for the management API."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from management import APIClient, ManagementError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="torrentctl", description="Manage one running torrent-client instance")
    parser.add_argument(
        "--address", default=os.getenv("TORRENT_CLIENT_ADDRESS"),
        help="management API origin, e.g. http://127.0.0.1:8080 (or TORRENT_CLIENT_ADDRESS)",
    )
    parser.add_argument(
        "--token", default=os.getenv("TORRENT_CLIENT_TOKEN"),
        help="bearer token (or TORRENT_CLIENT_TOKEN)",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status", help="show instance status")
    commands.add_parser("list", help="list live torrents")
    get_parser = commands.add_parser("get", help="show one live torrent")
    get_parser.add_argument("info_hash")
    add_parser = commands.add_parser("add", help="asynchronously add a magnet URI or info hash")
    add_parser.add_argument("source")
    for action in ("pause", "resume", "reannounce"):
        action_parser = commands.add_parser(action, help=f"{action} one torrent or all")
        action_parser.add_argument("selector", help="info hash or the literal 'all'")
    return parser


def execute(client: APIClient, args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "status":
        return client.request("GET", "/status")
    if args.command == "list":
        return client.request("GET", "/torrents")
    if args.command == "get":
        return client.request("GET", f"/torrents/{args.info_hash}")
    if args.command == "add":
        return client.request("POST", "/torrents", {"source": args.source})
    return client.request("POST", f"/torrents/{args.selector}/{args.command}", {})


def human_output(command: str, payload: dict[str, Any]) -> str:
    if command == "status":
        return (
            f"instance: {payload['instance_id']}\nmode: {payload['mode']}\n"
            f"live torrents: {payload['live_torrent_count']}/{payload['capacity']}"
        )
    if command == "list":
        torrents = payload["torrents"]
        if not torrents:
            return "No live torrents."
        return "\n".join(
            f"{item['info_hash']}  {item['progress'] * 100:6.2f}%  {item['name']}" for item in torrents
        )
    if command == "get":
        item = payload["torrent"]
        return "\n".join(f"{key}: {value}" for key, value in item.items())
    if command == "add":
        return f"accepted: {payload['info_hash']}"
    return f"{payload['action']}: {payload['affected']} torrent(s) ({payload['selector']})"


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.address:
        parser.error("--address or TORRENT_CLIENT_ADDRESS is required")
    if not args.token:
        parser.error("--token or TORRENT_CLIENT_TOKEN is required")
    try:
        payload = execute(APIClient(args.address, args.token), args)
    except ManagementError as exc:
        print(f"torrentctl: {exc.code}: {exc.message}", file=sys.stderr)
        return 4 if exc.code == "connection_error" else 3
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        print(human_output(args.command, payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
