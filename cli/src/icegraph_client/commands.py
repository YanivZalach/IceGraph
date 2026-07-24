from __future__ import annotations

import argparse
import sys
import webbrowser

from .client import IcegraphClient, IcegraphError, JobFailedError
from .config import CliConfig
from .storage import LocalStorage

VALID_PAGES = ("graph", "metadata", "timeline", "filetree")
VALID_TYPES = ("main_metadata", "metadata", "snapshot", "manifest", "data", "position_delete", "equality_delete")


class CommandRunner:
    def __init__(self, config: CliConfig):
        self._config = config
        self._client = IcegraphClient(config.server_url)
        self._storage = LocalStorage(config.data_dir)

    def tables(self, args: argparse.Namespace) -> int:
        try:
            response = self._client.list_tables()
        except IcegraphError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        for table in response.tables:
            print(table)
        return 0

    def load(self, args: argparse.Namespace) -> int:
        try:
            result = self._client.load_table(args.table, args.start, args.end)
        except (IcegraphError, JobFailedError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        path = self._storage.save(args.table, args.start, args.end, result)
        node_count = len(result.get("nodes", []))
        print(f"Loaded {node_count} nodes for {args.table} -> {path}")
        return 0

    def show(self, args: argparse.Namespace) -> int:
        try:
            result = self._storage.load(args.table, args.start, args.end)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        nodes = result.get("nodes", [])
        if args.type:
            nodes = [n for n in nodes if n.get("type") == args.type]
        if args.operation:
            nodes = [n for n in nodes if args.operation.lower() in (n.get("details", {}).get("operation") or "").lower()]

        if not nodes:
            print("No matching nodes.")
            return 0

        for node in nodes:
            operation = node.get("details", {}).get("operation")
            suffix = f"  [{operation}]" if operation else ""
            print(f"{node.get('type', '?'):<16} {node.get('id', '?')}{suffix}")
        return 0

    def open(self, args: argparse.Namespace) -> int:
        params = [f"table={args.table}"]
        if args.start is not None:
            params.append(f"start_snapshot_id={args.start}")
        if args.end is not None:
            params.append(f"end_snapshot_id={args.end}")

        url = f"{self._config.server_url}/table/{args.page}?{'&'.join(params)}"
        print(url)

        if not args.no_browser:
            webbrowser.open(url)
        return 0
