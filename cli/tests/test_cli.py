import pytest

from icegraph_client.__main__ import build_parser


def test_tables_subcommand_parses():
    args = build_parser().parse_args(["tables"])
    assert args.command == "tables"


def test_global_flags_parse_before_subcommand():
    args = build_parser().parse_args(["--server", "http://myhost:9000", "--data-dir", "/data", "tables"])
    assert args.server == "http://myhost:9000"
    assert args.data_dir == "/data"


def test_non_interactive_flag_defaults_to_false():
    args = build_parser().parse_args(["tables"])
    assert args.non_interactive is False


def test_non_interactive_flag_parses_before_subcommand():
    args = build_parser().parse_args(["--non-interactive", "tables"])
    assert args.non_interactive is True


def test_load_subcommand_parses_range():
    args = build_parser().parse_args(["load", "default.logging", "--start", "1", "--end", "2"])
    assert args.table == "default.logging"
    assert args.start == "1"
    assert args.end == "2"


def test_load_subcommand_json_flag():
    args = build_parser().parse_args(["load", "default.logging", "--json"])
    assert args.json is True

    default_args = build_parser().parse_args(["load", "default.logging"])
    assert default_args.json is False


def test_load_subcommand_json_short_flag():
    args = build_parser().parse_args(["load", "default.logging", "-j"])
    assert args.json is True


def test_load_subcommand_accepts_timestamp_range():
    args = build_parser().parse_args(["load", "default.logging", "--start", "2026-01-01", "--end", "2026-02-01T00:00:00"])
    assert args.start == "2026-01-01"
    assert args.end == "2026-02-01T00:00:00"


def test_use_subcommand_parses_table():
    args = build_parser().parse_args(["use", "default.logging"])
    assert args.command == "use"
    assert args.table == "default.logging"
    assert args.index is None


def test_use_subcommand_rejects_start_end():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["use", "default.logging", "--start", "1"])


def test_use_subcommand_index_flag():
    args = build_parser().parse_args(["use", "default.logging", "--index", "2"])
    assert args.index == 2


def test_use_subcommand_index_short_flag():
    args = build_parser().parse_args(["use", "default.logging", "-i", "0"])
    assert args.index == 0


def test_show_subcommand_rejects_start_end():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["show", "default.logging", "--start", "1"])


def test_open_subcommand_rejects_start_end():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["open", "default.logging", "--start", "1"])


def test_snapshots_subcommand_parses():
    args = build_parser().parse_args(["snapshots", "default.logging"])
    assert args.command == "snapshots"
    assert args.table == "default.logging"
    assert args.json is False


def test_snapshots_subcommand_json_short_flag():
    args = build_parser().parse_args(["snapshots", "default.logging", "-j"])
    assert args.json is True


def test_metadata_subcommand_parses():
    args = build_parser().parse_args(["metadata", "default.logging"])
    assert args.command == "metadata"
    assert args.table == "default.logging"
    assert args.json is False


def test_metadata_subcommand_json_short_flag():
    args = build_parser().parse_args(["metadata", "default.logging", "-j"])
    assert args.json is True


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


def test_show_subcommand_json_short_flag():
    args = build_parser().parse_args(["show", "default.logging", "-j"])
    assert args.json is True


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
