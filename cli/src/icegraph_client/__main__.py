import argparse
import sys
from typing import List, Optional

from icegraph_client.commands.commands import VALID_PAGES, VALID_TYPES, CommandRunner
from icegraph_client.config.config import CliConfig, MissingServerUrlError
from icegraph_client.storage.storage import DEFAULT_DATA_DIR


def _add_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--start", default=None, dest="start",
        help="Start snapshot ID, or a timestamp (or unambiguous prefix of one) from `icegraph snapshots <table>`",
    )
    parser.add_argument(
        "--end", default=None, dest="end",
        help="End snapshot ID, or a timestamp (or unambiguous prefix of one) from `icegraph snapshots <table>`",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icegraph", description="Inspect Apache Iceberg table metadata from the terminal.")
    parser.add_argument("--server", default=None, help="IceGraph server URL (env ICEGRAPH_SERVER_URL; prompted for and saved on first run if neither is set)")
    parser.add_argument("--data-dir", default=None, dest="data_dir", help=f"Local persistence directory (env ICEGRAPH_DATA_DIR, default {DEFAULT_DATA_DIR})")
    parser.add_argument(
        "--non-interactive", action="store_true", dest="non_interactive",
        help="Never prompt for input (env ICEGRAPH_NON_INTERACTIVE); fail immediately if --server/ICEGRAPH_SERVER_URL isn't set and none is saved yet. "
             "Also implied automatically when stdin isn't a terminal.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("tables", help="List Iceberg tables known to the server")

    snapshots_parser = subparsers.add_parser("snapshots", help="List a table's snapshot history (timestamp, id, operation)")
    snapshots_parser.add_argument("table")
    snapshots_parser.add_argument("-j", "--json", action="store_true", help="Output machine-readable JSON instead of formatted text")

    metadata_parser = subparsers.add_parser("metadata", help="Show a table's current metadata (schema, partition spec, sort order)")
    metadata_parser.add_argument("table")
    metadata_parser.add_argument("-j", "--json", action="store_true", help="Output compact JSON instead of pretty-printed")

    load_parser = subparsers.add_parser("load", help="Load a table's metadata and persist it to disk")
    load_parser.add_argument("table")
    _add_range_args(load_parser)
    load_parser.add_argument("-j", "--json", action="store_true", help="Also print the loaded result as machine-readable JSON (one-shot load+show)")

    use_parser = subparsers.add_parser("use", help="Switch the loaded range that show/metadata/open operate on, or list loaded ranges")
    use_parser.add_argument("table")
    _add_range_args(use_parser)

    show_parser = subparsers.add_parser("show", help="Explore the currently loaded table's nodes")
    show_parser.add_argument("table")
    show_parser.add_argument("--type", choices=VALID_TYPES, default=None)
    show_parser.add_argument("--operation", default=None)
    show_parser.add_argument("-j", "--json", action="store_true", help="Output machine-readable JSON instead of formatted text")
    show_mode_group = show_parser.add_mutually_exclusive_group()
    show_mode_group.add_argument("--issues", action="store_true", help="Show errors/warnings instead of nodes")
    show_mode_group.add_argument("--node", metavar="NODE_ID", default=None, help="Show full details for one node (matches an exact id or a unique substring of its id/label)")
    show_mode_group.add_argument("--children", metavar="NODE_ID", default=None, help="List the children of one node (matches an exact id or a unique substring of its id/label)")

    open_parser = subparsers.add_parser("open", help="Open the browser URL for the currently loaded table/range in the default browser")
    open_parser.add_argument("table")
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
