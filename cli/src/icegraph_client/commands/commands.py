import argparse
import itertools
import json
import sys
import webbrowser

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

    def load(self, args: argparse.Namespace) -> int:
        print(f"Loading {args.table} ...", file=sys.stderr)
        spinner = _Spinner()

        try:
            result = self._client.load_table(
                args.table,
                args.start,
                args.end,
                on_poll=lambda: spinner.tick(f"Loading {args.table} ..."),
            )
        except IcegraphError as e:
            spinner.clear()
            print(f"Error: {e}", file=sys.stderr)
            return 1

        spinner.clear()

        path = self._storage.save(args.table, args.start, args.end, result)
        node_count = len(result.get("nodes", []))
        print(f"Loaded {node_count} nodes for {args.table} -> {path}")

        issue_count = len(result.get("errors") or {}) + len(result.get("warnings") or {})
        if issue_count:
            print(f"{issue_count} issue(s) found -- run `icegraph show {args.table} --issues` to view them.")
        return 0

    def show(self, args: argparse.Namespace) -> int:
        try:
            result = self._storage.load(args.table, args.start, args.end)
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
            nodes = [n for n in nodes if n.get("type") == args.type]
        if args.operation:
            nodes = [n for n in nodes if args.operation.lower() in (n.get("details", {}).get("operation") or "").lower()]

        if args.json:
            print(json.dumps(nodes if filtered else result))
            return 0

        if not nodes:
            print("No matching nodes.")
            return 0

        for node in nodes:
            print(self._node_summary(node))
        return 0

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
        params = [f"table={args.table}"]
        if args.start is not None:
            params.append(f"start_snapshot_id={args.start}")
        if args.end is not None:
            params.append(f"end_snapshot_id={args.end}")

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
