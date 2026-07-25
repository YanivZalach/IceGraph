import argparse
import itertools
import json
import re
import sys
import webbrowser
from datetime import datetime
from typing import Optional, Union

from icegraph_client.client.client import IcegraphClient, IcegraphError
from icegraph_client.config.config import CliConfig
from icegraph_client.storage.storage import LocalStorage

VALID_PAGES = ("graph", "metadata", "timeline", "filetree")
VALID_TYPES = ("main_metadata", "metadata", "snapshot", "manifest", "data", "position_delete", "equality_delete")


class _Spinner:
    _FRAMES = "|/-\\"

    def __init__(self):
        self._enabled = sys.stderr.isatty()
        self._frames = itertools.cycle(self._FRAMES)
        self._last_len = 0

    def tick(self, message: str) -> None:
        if not self._enabled:
            return
        line = f"{next(self._frames)} {message}"
        self._last_len = len(line)
        sys.stderr.write(f"\r{line}")
        sys.stderr.flush()

    def clear(self) -> None:
        if not self._enabled or not self._last_len:
            return
        sys.stderr.write("\r" + " " * self._last_len + "\r")
        sys.stderr.flush()
        self._last_len = 0


class CommandRunner:
    def __init__(self, config: CliConfig):
        self._config = config
        self._client = IcegraphClient(config.server_url)
        self._storage = LocalStorage(config.data_dir)

    def tables(self, args: argparse.Namespace) -> int:
        print(f"Fetching tables from {self._config.server_url} ...", file=sys.stderr)

        try:
            response = self._client.list_tables()
        except IcegraphError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        for table in response.tables:
            print(table)
        return 0

    def metadata(self, args: argparse.Namespace) -> int:
        try:
            result = self._storage.load(args.table)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        table_metadata = result.get("metadata") or {}
        print(json.dumps(table_metadata) if args.json else json.dumps(table_metadata, indent=2, default=str))
        return 0

    def load(self, args: argparse.Namespace) -> int:
        spinner = _Spinner()
        spinner.tick(f"Loading {args.table} ...")

        try:
            start = self._resolve_snapshot_ref(args.table, args.start, "start")
            end = self._resolve_snapshot_ref(args.table, args.end, "end")
            result = self._client.load_table(
                args.table,
                start,
                end,
                on_poll=lambda: spinner.tick(f"Loading {args.table} ..."),
            )
        except IcegraphError as e:
            spinner.clear()
            print(f"Error: {e}", file=sys.stderr)
            return 1

        spinner.clear()

        path = self._storage.save(args.table, start, end, result)
        node_count = len(result.get("nodes", []))
        print(f"Loaded {node_count} nodes for {args.table} -> {path}")

        issue_count = len(result.get("errors") or {}) + len(result.get("warnings") or {})
        if issue_count:
            print(f"{issue_count} issue(s) found -- run `icegraph show {args.table} --issues` to view them.")
        return 0

    def snapshots(self, args: argparse.Namespace) -> int:
        try:
            snapshot_map = self._client.snapshot_map(args.table)
        except IcegraphError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        if args.json:
            print(json.dumps(snapshot_map))
            return 0

        for timestamp, info in snapshot_map.items():
            print(f"{timestamp}  {info.get('snapshot_id', '?'):<20} {info.get('operation', '?')}")
        return 0

    def use(self, args: argparse.Namespace) -> int:
        if args.start is None and args.end is None:
            return self._list_ranges(args.table)

        try:
            start = self._resolve_snapshot_ref(args.table, args.start, "start")
            end = self._resolve_snapshot_ref(args.table, args.end, "end")
            path = self._storage.set_latest(args.table, start, end)
        except (IcegraphError, FileNotFoundError) as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        print(f"Now using {args.table} {self._range_label(start)}-{self._range_label(end)} -> {path}")
        return 0

    def _list_ranges(self, table: str) -> int:
        ranges = self._storage.list_ranges(table)
        if not ranges:
            print(f"No loaded ranges for {table}. Run `icegraph load {table}` first.")
            return 0

        current = self._storage.current_range(table)
        for start, end in ranges:
            marker = "  (current)" if (start, end) == current else ""
            print(f"{self._range_label(start)} -> {self._range_label(end)}{marker}")
        return 0

    @staticmethod
    def _range_label(value: Optional[int]) -> str:
        return str(value) if value is not None else "None"

    def show(self, args: argparse.Namespace) -> int:
        try:
            result = self._storage.load(args.table)
        except FileNotFoundError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

        if args.issues:
            return self._show_issues(result, as_json=args.json)
        if args.node:
            return self._show_node(result, args.node, as_json=args.json)
        if args.children:
            return self._show_children(result, args.children, as_json=args.json)

        nodes = result.get("nodes", [])
        filtered = bool(args.type or args.operation)
        if args.type:
            matching_types = ("metadata", "main_metadata") if args.type == "metadata" else (args.type,)
            nodes = [n for n in nodes if n.get("type") in matching_types]
        if args.operation:
            nodes = [n for n in nodes if args.operation.lower() in (n.get("details", {}).get("operation") or "").lower()]

        if args.json:
            if filtered:
                print(json.dumps(nodes))
            else:
                print(json.dumps({k: v for k, v in result.items() if k != "metadata"}))
            return 0

        if not nodes:
            print("No matching nodes.")
            return 0

        for node in nodes:
            print(self._node_summary(node))
        return 0

    _DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    def _resolve_snapshot_ref(self, table: str, value: Optional[Union[str, int]], boundary: str) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int) or str(value).isdigit():
            return int(value)

        target = self._parse_timestamp(str(value), boundary)
        snapshot_map = self._client.snapshot_map(table)

        snapshots = []
        for ts, info in snapshot_map.items():
            try:
                snapshots.append((self._parse_timestamp(ts, boundary), int(info["snapshot_id"])))
            except IcegraphError:
                continue

        if boundary == "end":
            candidates = [(t, sid) for t, sid in snapshots if t <= target]
            pick = max(candidates, key=lambda pair: pair[0], default=None)
            if pick is None:
                raise IcegraphError(f"No snapshot found at or before '{value}' for {table}.")
        else:
            candidates = [(t, sid) for t, sid in snapshots if t >= target]
            pick = min(candidates, key=lambda pair: pair[0], default=None)
            if pick is None:
                raise IcegraphError(f"No snapshot found at or after '{value}' for {table}.")

        return pick[1]

    @classmethod
    def _parse_timestamp(cls, value: str, boundary: str) -> datetime:
        text = value.strip()
        date_only = bool(cls._DATE_ONLY_RE.match(text))
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as e:
            raise IcegraphError(f"Could not parse '{value}' as a snapshot id or timestamp.") from e

        if date_only and boundary == "end":
            parsed = parsed.replace(hour=23, minute=59, second=59, microsecond=999999)

        # The backend reports snapshot timestamps in UTC, but a value with no explicit
        # offset was typed by the user in their own local timezone, not UTC.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=cls._local_timezone())

        return parsed

    @staticmethod
    def _local_timezone():
        return datetime.now().astimezone().tzinfo

    @staticmethod
    def _node_summary(node: dict) -> str:
        operation = node.get("details", {}).get("operation")
        suffix = f"  [{operation}]" if operation else ""
        return f"{node.get('type', '?'):<16} {node.get('id', '?')}{suffix}"

    @staticmethod
    def _find_node(nodes, query):
        for node in nodes:
            if node.get("id") == query:
                return node, None

        query_lower = query.lower()
        matches = [n for n in nodes if query_lower in (n.get("id") or "").lower() or query_lower in (n.get("label") or "").lower()]

        if len(matches) == 1:
            return matches[0], None
        if not matches:
            return None, f"No node matches '{query}'."

        candidates = "\n".join(f"  {n.get('type', '?'):<16} {n.get('id', '?')}" for n in matches[:20])
        return None, f"'{query}' matches multiple nodes, be more specific:\n{candidates}"

    @staticmethod
    def _show_issues(result: dict, as_json: bool = False) -> int:
        errors = result.get("errors") or {}
        warnings = result.get("warnings") or {}

        if as_json:
            print(json.dumps({"errors": errors, "warnings": warnings}))
            return 0

        if not errors and not warnings:
            print("No issues.")
            return 0

        for op, message in errors.items():
            print(f"ERROR    {op}: {message}")
        for op, message in warnings.items():
            print(f"WARNING  {op}: {message}")
        return 0

    def _show_node(self, result: dict, query: str, as_json: bool = False) -> int:
        node, error = self._find_node(result.get("nodes", []), query)
        if error:
            print(error, file=sys.stderr)
            return 1

        if as_json:
            print(json.dumps(node))
            return 0

        print(f"{node.get('type', '?')}  {node.get('id', '?')}")
        for key, value in (node.get("details") or {}).items():
            print(f"  {key}: {value}")
        return 0

    def _show_children(self, result: dict, query: str, as_json: bool = False) -> int:
        nodes = result.get("nodes", [])
        node, error = self._find_node(nodes, query)
        if error:
            print(error, file=sys.stderr)
            return 1

        id_to_node = {n.get("id"): n for n in nodes}
        children = [
            id_to_node[edge["to"]]
            for edge in result.get("edges", [])
            if edge.get("from") == node.get("id") and edge.get("to") in id_to_node
        ]

        if as_json:
            print(json.dumps(children))
            return 0

        if not children:
            print("No children.")
            return 0

        for child in children:
            print(self._node_summary(child))
        return 0

    def open(self, args: argparse.Namespace) -> int:
        current = self._storage.current_range(args.table)
        start, end = current if current is not None else (None, None)

        params = [f"table={args.table}"]
        if start is not None:
            params.append(f"start_snapshot_id={start}")
        if end is not None:
            params.append(f"end_snapshot_id={end}")

        url = f"{self._config.server_url}/table/{args.page}?{'&'.join(params)}"

        if args.no_browser:
            print(url)
            return 0

        try:
            opened = webbrowser.open(url)
        except webbrowser.Error:
            opened = False

        if not opened:
            print(url)
        return 0
