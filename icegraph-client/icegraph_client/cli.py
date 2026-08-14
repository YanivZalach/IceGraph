import argparse
import inspect
import os
import sys

import requests

from icegraph_client.clients.icegraph_client import IceGraphClient
from icegraph_client.utils.json_utils import jsonify

BASE_URL_ENV_VAR = "ICEGRAPH_BASE_URL"
TOKEN_ENV_VAR = "ICEGRAPH_TOKEN"
COOKIE_ENV_VAR = "ICEGRAPH_COOKIE"
NO_VERIFY_SSL_ENV_VAR = "ICEGRAPH_NO_VERIFY_SSL"


def _tables(client: IceGraphClient, _: argparse.Namespace):
    print("Fetching tables...", file=sys.stderr)
    return client.list_tables()


def _snapshots(client: IceGraphClient, args: argparse.Namespace):
    print(f"Fetching snapshots for '{args.table}'...", file=sys.stderr)
    return client.get_snapshot_map(args.table)


def _graph(client: IceGraphClient, args: argparse.Namespace):
    print(f"Building graph for '{args.table}'...", file=sys.stderr)
    return client.get_graph(args.table, args.start_snapshot_id, args.end_snapshot_id)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icegraph", description="CLI for the IceGraph server API")
    parser.add_argument("--base-url", default=None, help=f"IceGraph server URL. Falls back to the {BASE_URL_ENV_VAR} environment variable")
    parser.add_argument(
        "--token",
        default=None,
        help=f"Bearer token, for servers that require authentication. Falls back to the {TOKEN_ENV_VAR} environment variable",
    )
    parser.add_argument(
        "--cookie",
        default=None,
        help=f"Cookie header value, for servers that require authentication. Falls back to the {COOKIE_ENV_VAR} environment variable",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        default=False,
        help=f"Skip TLS certificate verification. Falls back to the {NO_VERIFY_SSL_ENV_VAR} environment variable",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    tables_parser = subparsers.add_parser("tables", help="List all available tables")
    tables_parser.set_defaults(handler=_tables)

    snapshots_parser = subparsers.add_parser("snapshots", help="Get the snapshot map for a table")
    snapshots_parser.add_argument("table", help="Full table name")
    snapshots_parser.set_defaults(handler=_snapshots)

    graph_parser = subparsers.add_parser("graph", help="Build the metadata graph for a table")
    graph_parser.add_argument("table", help="Full table name")
    graph_parser.add_argument("-s", "--start-snapshot-id", default=None, help="Start snapshot id")
    graph_parser.add_argument("-e", "--end-snapshot-id", default=None, help="End snapshot id")
    graph_parser.set_defaults(handler=_graph)

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

    token = args.token or os.environ.get(TOKEN_ENV_VAR)
    cookie = args.cookie or os.environ.get(COOKIE_ENV_VAR)
    no_verify_ssl = args.no_verify_ssl or os.environ.get(NO_VERIFY_SSL_ENV_VAR, "").strip().lower() in ("1", "true", "yes")

    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if cookie:
        headers["Cookie"] = cookie

    client = IceGraphClient(base_url, headers=headers, verify=not no_verify_ssl)

    try:
        result = args.handler(client, args)
    except requests.RequestException as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print(jsonify(result))


if __name__ == "__main__":
    main()
