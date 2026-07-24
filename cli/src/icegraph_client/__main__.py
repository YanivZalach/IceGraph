from __future__ import annotations

import argparse
import sys
from typing import Optional

from .client import DEFAULT_SERVER_URL
from .commands import VALID_PAGES, VALID_TYPES, CommandRunner
from .config import CliConfig
from .storage import DEFAULT_DATA_DIR


def _add_range_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", type=int, default=None, dest="start", help="Start snapshot ID")
    parser.add_argument("--end", type=int, default=None, dest="end", help="End snapshot ID")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="icegraph", description="Inspect Apache Iceberg table metadata from the terminal.")
    parser.add_argument("--server", default=None, help=f"IceGraph server URL (env ICEGRAPH_SERVER_URL, default {DEFAULT_SERVER_URL})")
    parser.add_argument("--data-dir", default=None, dest="data_dir", help=f"Local persistence directory (env ICEGRAPH_DATA_DIR, default {DEFAULT_DATA_DIR})")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("tables", help="List Iceberg tables known to the server")

    load_parser = subparsers.add_parser("load", help="Load a table's metadata and persist it to disk")
    load_parser.add_argument("table")
    _add_range_args(load_parser)

    show_parser = subparsers.add_parser("show", help="Print a previously loaded table's nodes")
    show_parser.add_argument("table")
    _add_range_args(show_parser)
    show_parser.add_argument("--type", choices=VALID_TYPES, default=None)
    show_parser.add_argument("--operation", default=None)

    open_parser = subparsers.add_parser("open", help="Print (and open) the browser URL for a table")
    open_parser.add_argument("table")
    _add_range_args(open_parser)
    open_parser.add_argument("--page", choices=VALID_PAGES, default="graph")
    open_parser.add_argument("--no-browser", action="store_true", dest="no_browser")

    return parser


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = CliConfig.from_args(args)
    runner = CommandRunner(config)
    method = getattr(runner, args.command)
    return method(args)


if __name__ == "__main__":
    sys.exit(main())
