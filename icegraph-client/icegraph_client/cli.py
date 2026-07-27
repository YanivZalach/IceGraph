import argparse
import inspect
import os
import sys

from icegraph_client.clients.icegraph_client import IceGraphClient
from icegraph_client.utils.json_utils import jsonify

BASE_URL_ENV_VAR = "ICEGRAPH_BASE_URL"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icegraph")
    parser.add_argument("--base-url", default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("tables")

    snapshots_parser = subparsers.add_parser("snapshots")
    snapshots_parser.add_argument("table")

    graph_parser = subparsers.add_parser("graph")
    graph_parser.add_argument("table")
    graph_parser.add_argument("--start-snapshot-id", default=None)
    graph_parser.add_argument("--end-snapshot-id", default=None)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    base_url = args.base_url or os.environ.get(BASE_URL_ENV_VAR)
    if not base_url:
        print(
            inspect.cleandoc(f"""
                No IceGraph server address was given.

                Set it with one of the following:
                  As an argument: --base-url http://<icegraph-server-host>
                  As an environment variable (bash/zsh): export {BASE_URL_ENV_VAR}=http://<icegraph-server-host>
                  As an environment variable (cmd): set {BASE_URL_ENV_VAR}=http://<icegraph-server-host>
            """),
            file=sys.stderr,
        )
        sys.exit(2)

    client = IceGraphClient(base_url)

    if args.command == "tables":
        print("Fetching tables...", file=sys.stderr)
        result = client.list_tables()

    elif args.command == "snapshots":
        print(f"Fetching snapshots for '{args.table}'...", file=sys.stderr)
        result = client.get_snapshot_map(args.table)

    elif args.command == "graph":
        print(f"Building graph for '{args.table}'...", file=sys.stderr)
        result = client.get_graph(args.table, args.start_snapshot_id, args.end_snapshot_id)

    print(jsonify(result))


if __name__ == "__main__":
    main()
