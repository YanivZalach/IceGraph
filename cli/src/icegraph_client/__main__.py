import argparse
import sys
from typing import List, Optional

from icegraph_client.commands.commands import VALID_PAGES, VALID_TYPES, CommandRunner
from icegraph_client.config.config import CliConfig, MissingServerUrlError
from icegraph_client.storage.storage import DEFAULT_DATA_DIR


def _add_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", type=int, default=None, dest="start", help="Start snapshot ID")
    parser.add_argument("--end", type=int, default=None, dest="end", help="End snapshot ID")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icegraph", description="Inspect Apache Iceberg table metadata from the terminal.")
    parser.add_argument("--server", default=None, help="IceGraph server URL (env ICEGRAPH_SERVER_URL; prompted for and saved on first run if neither is set)")
    parser.add_argument("--data-dir", default=None, dest="data_dir", help=f"Local persistence directory (env ICEGRAPH_DATA_DIR, default {DEFAULT_DATA_DIR})")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("tables", help="List Iceberg tables known to the server")

    load_parser = subparsers.add_parser("load", help="Load a table's metadata and persist it to disk")
    load_parser.add_argument("table")
    _add_range_args(load_parser)

    show_parser = subparsers.add_parser("show", help="Explore a previously loaded table's nodes")
    show_parser.add_argument("table")
    _add_range_args(show_parser)
    show_parser.add_argument("--type", choices=VALID_TYPES, default=None)
    show_parser.add_argument("--operation", default=None)
    show_parser.add_argument("--json", action="store_true", help="Output machine-readable JSON instead of formatted text")
    show_mode_group = show_parser.add_mutually_exclusive_group()
    show_mode_group.add_argument("--issues", action="store_true", help="Show errors/warnings instead of nodes")
    show_mode_group.add_argument("--node", metavar="NODE_ID", default=None, help="Show full details for one node (matches an exact id or a unique substring of its id/label)")
    show_mode_group.add_argument("--children", metavar="NODE_ID", default=None, help="List the children of one node (matches an exact id or a unique substring of its id/label)")

    open_parser = subparsers.add_parser("open", help="Open the browser URL for a table in the default browser")
    open_parser.add_argument("table")
    _add_range_args(open_parser)
    open_parser.add_argument("--page", choices=VALID_PAGES, default="graph")
    open_parser.add_argument("--no-browser", action="store_true", dest="no_browser", help="Print the URL instead of opening it")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = CliConfig.from_args(args)
    except MissingServerUrlError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    runner = CommandRunner(config)
    method = getattr(runner, args.command)
    return method(args)


if __name__ == "__main__":
    sys.exit(main())
