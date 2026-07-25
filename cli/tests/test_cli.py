import pytest

from icegraph_client.__main__ import build_parser


def test_tables_subcommand_parses():
    args = build_parser().parse_args(["tables"])
    assert args.command == "tables"


def test_global_flags_parse_before_subcommand():
    args = build_parser().parse_args(["--server", "http://myhost:9000", "--data-dir", "/data", "tables"])
    assert args.server == "http://myhost:9000"
    assert args.data_dir == "/data"


def test_load_subcommand_parses_range():
    args = build_parser().parse_args(["load", "default.logging", "--start", "1", "--end", "2"])
    assert args.table == "default.logging"
    assert args.start == 1
    assert args.end == 2


def test_show_subcommand_parses_filters():
    args = build_parser().parse_args(["show", "default.logging", "--type", "snapshot", "--operation", "append"])
    assert args.type == "snapshot"
    assert args.operation == "append"


def test_show_subcommand_issues_flag():
    args = build_parser().parse_args(["show", "default.logging", "--issues"])
    assert args.issues is True

    default_args = build_parser().parse_args(["show", "default.logging"])
    assert default_args.issues is False


def test_show_subcommand_node_flag():
    args = build_parser().parse_args(["show", "default.logging", "--node", "snap-1"])
    assert args.node == "snap-1"
    assert args.children is None


def test_show_subcommand_children_flag():
    args = build_parser().parse_args(["show", "default.logging", "--children", "manifest-1"])
    assert args.children == "manifest-1"
    assert args.node is None


def test_show_subcommand_json_flag():
    args = build_parser().parse_args(["show", "default.logging", "--json"])
    assert args.json is True

    default_args = build_parser().parse_args(["show", "default.logging"])
    assert default_args.json is False


def test_show_subcommand_node_and_children_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["show", "default.logging", "--node", "a", "--children", "b"])


def test_show_subcommand_node_and_issues_are_mutually_exclusive():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["show", "default.logging", "--node", "a", "--issues"])


def test_open_subcommand_defaults_to_graph_page():
    args = build_parser().parse_args(["open", "default.logging"])
    assert args.page == "graph"
    assert args.no_browser is False


def test_open_subcommand_no_browser_flag():
    args = build_parser().parse_args(["open", "default.logging", "--no-browser"])
    assert args.no_browser is True
