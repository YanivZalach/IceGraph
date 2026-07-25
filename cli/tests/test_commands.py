import argparse
import json
import sys

import pytest

from icegraph_client.client.client import IcegraphError
from icegraph_client.commands.commands import CommandRunner
from icegraph_client.config.config import CliConfig
from icegraph_client.storage.storage import EMPTY_TABLE_END


@pytest.fixture(autouse=True)
def _fixed_local_timezone(monkeypatch):
    # Snapshot resolution treats naive (no explicit offset) user input as local time.
    # Pin "local" to UTC by default so date-only tests are deterministic regardless
    # of the timezone of the machine running the suite.
    monkeypatch.setattr(CommandRunner, "_local_timezone", staticmethod(lambda: "UTC"))


def _show_args(table, type_=None, operation=None, issues=False, node=None, children=None, json_=False):
    return argparse.Namespace(
        table=table,
        type=type_,
        operation=operation,
        issues=issues,
        node=node,
        children=children,
        json=json_,
    )


def _open_args(table, page="graph", no_browser=True):
    return argparse.Namespace(table=table, page=page, no_browser=no_browser)


def _load_args(table, start=None, end=None, json_=False):
    return argparse.Namespace(table=table, start=start, end=end, json=json_)


def _use_args(table, start=None, end=None, index=None):
    return argparse.Namespace(table=table, start=start, end=end, index=index)


def _snapshots_args(table, json_=False):
    return argparse.Namespace(table=table, json=json_)


def test_cmd_show_prints_all_nodes_by_default(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [
        {"id": "a", "type": "metadata"},
        {"id": "b", "type": "snapshot", "details": {"operation": "append"}},
    ]
    runner._storage.save("default.logging", None, None, {"nodes": nodes})

    exit_code = runner.show(_show_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "metadata" in out
    assert "snapshot" in out
    assert "[append]" in out


def test_cmd_show_filters_by_type(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [
        {"id": "a", "type": "metadata"},
        {"id": "b", "type": "snapshot", "details": {"operation": "append"}},
    ]
    runner._storage.save("default.logging", None, None, {"nodes": nodes})

    exit_code = runner.show(_show_args("default.logging", type_="snapshot"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "metadata" not in out
    assert "snapshot" in out


def test_cmd_show_type_metadata_also_includes_main_metadata(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [
        {"id": "a", "type": "main_metadata"},
        {"id": "b", "type": "metadata"},
        {"id": "c", "type": "snapshot", "details": {"operation": "append"}},
    ]
    runner._storage.save("default.logging", None, None, {"nodes": nodes})

    exit_code = runner.show(_show_args("default.logging", type_="metadata"))

    out = capsys.readouterr().out
    lines = [line.split() for line in out.strip().splitlines()]
    assert exit_code == 0
    assert lines == [["main_metadata", "a"], ["metadata", "b"]]


def test_cmd_show_filters_by_operation_case_insensitive(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [
        {"id": "a", "type": "snapshot", "details": {"operation": "append"}},
        {"id": "b", "type": "snapshot", "details": {"operation": "overwrite"}},
    ]
    runner._storage.save("default.logging", None, None, {"nodes": nodes})

    exit_code = runner.show(_show_args("default.logging", operation="APPEND"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "append" in out
    assert "overwrite" not in out


def test_cmd_show_reports_no_matches(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", None, None, {"nodes": [{"id": "a", "type": "metadata"}]})

    exit_code = runner.show(_show_args("default.logging", type_="snapshot"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "No matching nodes." in out


def test_cmd_show_errors_when_nothing_loaded(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    exit_code = runner.show(_show_args("default.logging"))

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "Run `icegraph load default.logging` first." in err


def test_cmd_show_issues_prints_errors_and_warnings(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    result = {
        "nodes": [],
        "errors": {"snapshot-1": "boom"},
        "warnings": {"snapshot-2": "careful"},
    }
    runner._storage.save("default.logging", None, None, result)

    exit_code = runner.show(_show_args("default.logging", issues=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "ERROR    snapshot-1: boom" in out
    assert "WARNING  snapshot-2: careful" in out


def test_cmd_show_issues_reports_none(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", None, None, {"nodes": []})

    exit_code = runner.show(_show_args("default.logging", issues=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "No issues." in out


def test_cmd_show_issues_json(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    result = {"nodes": [], "errors": {"snapshot-1": "boom"}, "warnings": {}}
    runner._storage.save("default.logging", None, None, result)

    exit_code = runner.show(_show_args("default.logging", issues=True, json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == {"errors": {"snapshot-1": "boom"}, "warnings": {}}


def test_cmd_show_node_prints_full_details(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [
        {"id": "s3://bucket/table/snap-1.avro", "label": "snap-1.avro", "type": "snapshot", "details": {"operation": "append", "record_count": 42}},
    ]
    runner._storage.save("default.logging", None, None, {"nodes": nodes})

    exit_code = runner.show(_show_args("default.logging", node="snap-1"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "snapshot" in out
    assert "operation: append" in out
    assert "record_count: 42" in out


def test_cmd_show_node_json(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    node = {"id": "a", "type": "snapshot", "details": {"operation": "append"}}
    runner._storage.save("default.logging", None, None, {"nodes": [node]})

    exit_code = runner.show(_show_args("default.logging", node="a", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == node


def test_cmd_show_node_not_found(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", None, None, {"nodes": [{"id": "a", "type": "metadata"}]})

    exit_code = runner.show(_show_args("default.logging", node="nonexistent"))

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "No node matches" in err


def test_cmd_show_node_ambiguous_match(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [
        {"id": "path/snap-1.avro", "type": "snapshot"},
        {"id": "path/snap-11.avro", "type": "snapshot"},
    ]
    runner._storage.save("default.logging", None, None, {"nodes": nodes})

    exit_code = runner.show(_show_args("default.logging", node="snap-1"))

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "matches multiple nodes" in err


def test_cmd_show_children_lists_child_nodes(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [
        {"id": "manifest-1", "type": "manifest"},
        {"id": "data-1", "type": "data", "details": {}},
        {"id": "data-2", "type": "data", "details": {}},
    ]
    edges = [
        {"from": "manifest-1", "to": "data-1"},
        {"from": "manifest-1", "to": "data-2"},
    ]
    runner._storage.save("default.logging", None, None, {"nodes": nodes, "edges": edges})

    exit_code = runner.show(_show_args("default.logging", children="manifest-1"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "data-1" in out
    assert "data-2" in out


def test_cmd_show_children_json(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    child = {"id": "data-1", "type": "data", "details": {}}
    nodes = [{"id": "manifest-1", "type": "manifest"}, child]
    edges = [{"from": "manifest-1", "to": "data-1"}]
    runner._storage.save("default.logging", None, None, {"nodes": nodes, "edges": edges})

    exit_code = runner.show(_show_args("default.logging", children="manifest-1", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == [child]


def test_cmd_show_children_reports_none(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [{"id": "data-1", "type": "data"}]
    runner._storage.save("default.logging", None, None, {"nodes": nodes, "edges": []})

    exit_code = runner.show(_show_args("default.logging", children="data-1"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "No children." in out


def test_cmd_show_json_without_filters_dumps_full_result_except_table_metadata(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    result = {"nodes": [{"id": "a", "type": "metadata"}], "edges": [], "metadata": {"schema": {}}, "errors": {}, "warnings": {}}
    runner._storage.save("default.logging", None, None, result)

    exit_code = runner.show(_show_args("default.logging", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    # the table's root "metadata" key is exclusive to `icegraph metadata` -- show never leaks it
    assert json.loads(out) == {"nodes": [{"id": "a", "type": "metadata"}], "edges": [], "errors": {}, "warnings": {}}


def test_cmd_show_json_with_type_filter_dumps_only_nodes(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [{"id": "a", "type": "metadata"}, {"id": "b", "type": "snapshot"}]
    runner._storage.save("default.logging", None, None, {"nodes": nodes, "edges": [], "metadata": {}})

    exit_code = runner.show(_show_args("default.logging", type_="snapshot", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == [{"id": "b", "type": "snapshot"}]


def test_cmd_open_prints_url_for_currently_loaded_range(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", 1, 2, {"nodes": []})

    exit_code = runner.open(_open_args("default.logging"))

    out = capsys.readouterr().out.strip()
    assert exit_code == 0
    assert out == "http://localhost:5000/table/graph?table=default.logging&start_snapshot_id=1&end_snapshot_id=2"


def test_cmd_open_omits_range_params_when_nothing_loaded(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    exit_code = runner.open(_open_args("default.logging"))

    out = capsys.readouterr().out.strip()
    assert exit_code == 0
    assert out == "http://localhost:5000/table/graph?table=default.logging"


def test_cmd_open_respects_configured_server_and_page(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://myhost:9000", data_dir=tmp_path))

    exit_code = runner.open(_open_args("default.logging", page="timeline"))

    out = capsys.readouterr().out.strip()
    assert exit_code == 0
    assert out == "http://myhost:9000/table/timeline?table=default.logging"


def test_cmd_open_opens_browser_and_does_not_print_by_default(tmp_path, capsys, monkeypatch):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    calls = []
    monkeypatch.setattr("webbrowser.open", lambda url: calls.append(url) or True)

    exit_code = runner.open(_open_args("default.logging", no_browser=False))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert out == ""
    assert calls == ["http://localhost:5000/table/graph?table=default.logging"]


def test_cmd_open_prints_when_no_browser_available(tmp_path, capsys, monkeypatch):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    monkeypatch.setattr("webbrowser.open", lambda url: False)

    exit_code = runner.open(_open_args("default.logging", no_browser=False))

    out = capsys.readouterr().out.strip()
    assert exit_code == 0
    assert out == "http://localhost:5000/table/graph?table=default.logging"


def test_cmd_open_prints_when_webbrowser_raises(tmp_path, capsys, monkeypatch):
    import webbrowser

    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    def _raise(url):
        raise webbrowser.Error("no browser")

    monkeypatch.setattr("webbrowser.open", _raise)

    exit_code = runner.open(_open_args("default.logging", no_browser=False))

    out = capsys.readouterr().out.strip()
    assert exit_code == 0
    assert out == "http://localhost:5000/table/graph?table=default.logging"


_ONE_SNAPSHOT = {"2026-01-01T00:00:00+00:00": {"snapshot_id": "1", "operation": "append"}}


def test_cmd_load_prints_loading_banner_only_once(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True)
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _ONE_SNAPSHOT
    runner._client.load_table = lambda *a, **kw: {"nodes": []}

    exit_code = runner.load(_load_args("default.logging"))

    err = capsys.readouterr().err
    assert exit_code == 0
    assert "\n" not in err
    assert err.count("Loading default.logging") == 1


def test_cmd_load_json_prints_only_json_on_stdout(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _ONE_SNAPSHOT
    runner._client.load_table = lambda *a, **kw: {"nodes": [{"id": "a"}], "edges": [], "metadata": {"schema": {}}, "errors": {}, "warnings": {}}

    exit_code = runner.load(_load_args("default.logging", json_=True))

    captured = capsys.readouterr()
    assert exit_code == 0
    assert json.loads(captured.out) == {"nodes": [{"id": "a"}], "edges": [], "errors": {}, "warnings": {}}
    assert "Loaded 1 nodes" in captured.err


def test_cmd_load_json_still_saves_to_storage(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _ONE_SNAPSHOT
    runner._client.load_table = lambda *a, **kw: {"nodes": []}

    runner.load(_load_args("default.logging", json_=True))

    assert runner._storage.load("default.logging") == {"nodes": []}


def test_cmd_load_without_json_prints_status_to_stdout(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _ONE_SNAPSHOT
    runner._client.load_table = lambda *a, **kw: {"nodes": []}

    exit_code = runner.load(_load_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "Loaded 0 nodes" in out


def test_cmd_load_omitted_start_stays_none_omitted_end_resolves_to_latest(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS
    calls = []

    def _load_table(table, start, end, on_poll=None):
        calls.append((start, end))
        return {"nodes": []}

    runner._client.load_table = _load_table

    runner.load(_load_args("default.logging"))

    # Omitted start means "from the beginning of history" -- a valid request left as
    # None. Omitted end always resolves to a concrete number: the actual latest snapshot.
    assert calls == [(None, 200)]


def test_resolve_snapshot_ref_none_start_stays_none(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    def _fail(table):
        raise AssertionError("an omitted start should never need the snapshot map")

    runner._client.snapshot_map = _fail

    assert runner._resolve_snapshot_ref("default.logging", None, "start") is None


def test_resolve_snapshot_ref_none_end_resolves_to_actual_latest(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    assert runner._resolve_snapshot_ref("default.logging", None, "end") == 200


def test_resolve_snapshot_ref_none_start_stays_none_for_freshly_created_table(tmp_path):
    # A table with only its initial metadata file (no snapshots yet) has nothing to
    # anchor an omitted start to either, but it never needed resolving in the first
    # place -- it's simply left as None.
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    def _fail(table):
        raise AssertionError("an omitted start should never need the snapshot map")

    runner._client.snapshot_map = _fail

    assert runner._resolve_snapshot_ref("default.logging", None, "start") is None


def test_resolve_snapshot_ref_none_end_marks_empty_for_freshly_created_table(tmp_path):
    # A table with only its initial metadata file (no snapshots yet) has no "latest
    # snapshot" to resolve end to -- mark it explicitly as empty rather than guessing.
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {}

    assert runner._resolve_snapshot_ref("default.logging", None, "end") == EMPTY_TABLE_END


def test_resolve_snapshot_ref_explicit_value_still_raises_when_table_has_no_snapshots(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {}

    with pytest.raises(IcegraphError, match="has no snapshots"):
        runner._resolve_snapshot_ref("default.logging", "2026-01-01", "end")

    with pytest.raises(IcegraphError, match="has no snapshots"):
        runner._resolve_snapshot_ref("default.logging", "2026-01-01", "start")


def test_resolve_snapshot_ref_accepts_int(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    assert runner._resolve_snapshot_ref("default.logging", 42, "start") == 42


def test_resolve_snapshot_ref_accepts_numeric_string_without_network(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    def _fail(table):
        raise AssertionError("should not call snapshot_map for a plain numeric id")

    runner._client.snapshot_map = _fail

    assert runner._resolve_snapshot_ref("default.logging", "42", "end") == 42


_THREE_SNAPSHOTS = {
    "2026-01-01T00:00:00+00:00": {"snapshot_id": "100", "operation": "append"},
    "2026-01-15T00:00:00+00:00": {"snapshot_id": "150", "operation": "append"},
    "2026-02-01T00:00:00+00:00": {"snapshot_id": "200", "operation": "append"},
}


def test_resolve_range_fetches_snapshot_map_only_once_for_both_bounds(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    calls = []

    def _snapshot_map(table):
        calls.append(table)
        return _THREE_SNAPSHOTS

    runner._client.snapshot_map = _snapshot_map

    start, end = runner._resolve_range("default.logging", "2026-01-01", "2026-02-01")

    assert (start, end) == (100, 200)
    assert calls == ["default.logging"]


def test_resolve_range_skips_network_when_both_bounds_are_numeric(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    def _fail(table):
        raise AssertionError("should not call snapshot_map when both bounds are plain ids")

    runner._client.snapshot_map = _fail

    assert runner._resolve_range("default.logging", "100", 200) == (100, 200)


def test_resolve_range_fetches_once_when_only_one_bound_is_a_timestamp(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    calls = []

    def _snapshot_map(table):
        calls.append(table)
        return _THREE_SNAPSHOTS

    runner._client.snapshot_map = _snapshot_map

    start, end = runner._resolve_range("default.logging", "2026-01-01", 999)

    assert (start, end) == (100, 999)
    assert calls == ["default.logging"]


def test_resolve_snapshot_ref_end_picks_latest_at_or_before(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    # 2026-01-20 falls between the 01-15 and 02-01 snapshots -> nearest at-or-before is 150
    assert runner._resolve_snapshot_ref("default.logging", "2026-01-20", "end") == 150


def test_resolve_snapshot_ref_start_picks_earliest_at_or_after(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    # 2026-01-20 falls between the 01-15 and 02-01 snapshots -> nearest at-or-after is 200
    assert runner._resolve_snapshot_ref("default.logging", "2026-01-20", "start") == 200


def test_resolve_snapshot_ref_date_only_end_includes_whole_day(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {
        "2026-01-15T08:00:00+00:00": {"snapshot_id": "150", "operation": "append"},
        "2026-01-15T20:00:00+00:00": {"snapshot_id": "151", "operation": "overwrite"},
    }

    # a bare date for --end should include everything committed that day, not just before midnight
    assert runner._resolve_snapshot_ref("default.logging", "2026-01-15", "end") == 151


def test_resolve_snapshot_ref_date_only_start_is_beginning_of_day(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {
        "2026-01-15T08:00:00+00:00": {"snapshot_id": "150", "operation": "append"},
        "2026-01-15T20:00:00+00:00": {"snapshot_id": "151", "operation": "overwrite"},
    }

    assert runner._resolve_snapshot_ref("default.logging", "2026-01-15", "start") == 150


def test_resolve_snapshot_ref_exact_timestamp_matches_itself(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    assert runner._resolve_snapshot_ref("default.logging", "2026-01-15T00:00:00+00:00", "end") == 150


def test_resolve_snapshot_ref_naive_input_uses_local_timezone_not_utc(tmp_path, monkeypatch):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    # Backend timestamps are always UTC. Simulate a user in UTC-8: local
    # "2026-01-15T20:00:00" (no offset given) is 2026-01-16T04:00:00 UTC.
    monkeypatch.setattr(CommandRunner, "_local_timezone", staticmethod(lambda: "-08:00"))
    runner._client.snapshot_map = lambda table: {
        "2026-01-16T03:00:00+00:00": {"snapshot_id": "150", "operation": "append"},
        "2026-01-16T05:00:00+00:00": {"snapshot_id": "151", "operation": "append"},
    }

    # If the naive input were misread as UTC (old, incorrect behavior), the target
    # would be 2026-01-15T20:00:00+00:00 -> before both snapshots -> would raise.
    # Read correctly as local UTC-8 -> target is 04:00 UTC -> nearest at-or-before is 150.
    assert runner._resolve_snapshot_ref("default.logging", "2026-01-15T20:00:00", "end") == 150


def test_resolve_snapshot_ref_explicit_offset_overrides_local_timezone(tmp_path, monkeypatch):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    monkeypatch.setattr(CommandRunner, "_local_timezone", staticmethod(lambda: "-08:00"))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    # An explicit +00:00 offset must be honored as-is, ignoring the local timezone.
    assert runner._resolve_snapshot_ref("default.logging", "2026-01-15T00:00:00+00:00", "end") == 150


def test_resolve_snapshot_ref_raises_when_end_before_all_snapshots(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    with pytest.raises(IcegraphError, match="No snapshot found at or before"):
        runner._resolve_snapshot_ref("default.logging", "2020-01-01", "end")


def test_resolve_snapshot_ref_raises_when_start_after_all_snapshots(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    with pytest.raises(IcegraphError, match="No snapshot found at or after"):
        runner._resolve_snapshot_ref("default.logging", "2030-01-01", "start")


def test_resolve_snapshot_ref_raises_on_unparseable_value(tmp_path):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: _THREE_SNAPSHOTS

    with pytest.raises(IcegraphError, match="Could not parse"):
        runner._resolve_snapshot_ref("default.logging", "not-a-date", "end")


def test_cmd_use_with_no_range_lists_loaded_ranges_with_index(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", 100, 200, {"nodes": []})
    runner._storage.save("default.logging", 300, 400, {"nodes": []})

    exit_code = runner.use(_use_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "[0] 100 -> 200" in out
    assert "[1] 300 -> 400  (current)" in out


def test_cmd_use_with_no_range_and_nothing_loaded(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    exit_code = runner.use(_use_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "Run `icegraph load default.logging` first." in out


def test_cmd_use_index_switches_to_that_range(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", 100, 200, {"nodes": ["first"]})
    runner._storage.save("default.logging", 300, 400, {"nodes": ["second"]})

    exit_code = runner.use(_use_args("default.logging", index=0))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "100-200" in out
    assert runner._storage.current_range("default.logging") == (100, 200)


def test_cmd_use_index_out_of_range_errors(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", 100, 200, {"nodes": []})

    exit_code = runner.use(_use_args("default.logging", index=5))

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "No range at index 5" in err


def test_cmd_use_index_when_nothing_loaded_errors(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    exit_code = runner.use(_use_args("default.logging", index=0))

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "No loaded ranges" in err


def test_cmd_use_ignores_stray_start_end_attributes(tmp_path, capsys):
    # use() only ever reads args.index now; a Namespace carrying leftover
    # start/end attributes (e.g. from argparse defaults) must not affect it.
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", 100, 200, {"nodes": ["first"]})
    runner._storage.save("default.logging", 300, 400, {"nodes": ["second"]})

    exit_code = runner.use(_use_args("default.logging", index=0, start=999, end=999))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "100-200" in out
    assert runner._storage.current_range("default.logging") == (100, 200)


def test_cmd_snapshots_prints_formatted_list(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {"2026-01-01T00:00:00+00:00": {"snapshot_id": "100", "operation": "append"}}

    exit_code = runner.snapshots(_snapshots_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "2026-01-01T00:00:00+00:00" in out
    assert "100" in out
    assert "append" in out


def test_cmd_snapshots_displays_timestamps_in_local_timezone(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(CommandRunner, "_local_timezone", staticmethod(lambda: "-08:00"))
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {"2026-01-01T00:00:00+00:00": {"snapshot_id": "100", "operation": "append"}}

    exit_code = runner.snapshots(_snapshots_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    # 2026-01-01T00:00:00 UTC displayed in UTC-8 is the previous day, 16:00
    assert "2025-12-31T16:00:00-08:00" in out
    assert "2026-01-01T00:00:00+00:00" not in out


def test_cmd_snapshots_json(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    snapshot_map = {"2026-01-01T00:00:00+00:00": {"snapshot_id": "100", "operation": "append"}}
    runner._client.snapshot_map = lambda table: snapshot_map

    exit_code = runner.snapshots(_snapshots_args("default.logging", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == snapshot_map


def test_cmd_snapshots_error(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    def _raise(table):
        raise IcegraphError("boom")

    runner._client.snapshot_map = _raise

    exit_code = runner.snapshots(_snapshots_args("default.logging"))

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "boom" in err


def _metadata_args(table, json_=False):
    return argparse.Namespace(table=table, json=json_)


def test_cmd_metadata_reads_from_currently_loaded_range(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    def _fail(*a, **kw):
        raise AssertionError("metadata must not hit the server -- it reads the loaded range like show/open")

    runner._client.snapshot_map = _fail
    runner._client.load_table = _fail
    runner._storage.save("default.logging", None, 200, {"metadata": {"table-name": "default.logging", "schemas": []}})

    exit_code = runner.metadata(_metadata_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == {"table-name": "default.logging", "schemas": []}


def test_cmd_metadata_follows_use_switch(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", 100, 200, {"metadata": {"schema-id": 1}})
    runner._storage.save("default.logging", 300, 400, {"metadata": {"schema-id": 2}})
    runner._storage.set_latest("default.logging", 100, 200)

    exit_code = runner.metadata(_metadata_args("default.logging"))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == {"schema-id": 1}


def test_cmd_metadata_json_is_compact(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.logging", None, None, {"metadata": {"a": 1}})

    exit_code = runner.metadata(_metadata_args("default.logging", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert out == '{"a": 1}\n'


def test_cmd_metadata_errors_when_nothing_loaded(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    exit_code = runner.metadata(_metadata_args("default.logging"))

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "Run `icegraph load default.logging` first." in err


def test_cmd_load_saves_empty_marker_and_warns_for_freshly_created_table(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {}
    runner._client.load_table = lambda table, start, end, on_poll=None: {"nodes": [{"id": "v1.metadata.json"}]}

    exit_code = runner.load(_load_args("default.fresh"))

    # load()'s status messages (including this note) go to whichever stream the
    # "Loaded N nodes" line uses -- stdout here since --json wasn't passed.
    out = capsys.readouterr().out
    assert exit_code == 0
    assert runner._storage.current_range("default.fresh") == (None, EMPTY_TABLE_END)
    assert "had no snapshots when this was loaded" in out
    assert "run `icegraph load default.fresh` again" in out


def test_cmd_load_json_warns_on_stderr_for_freshly_created_table(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._client.snapshot_map = lambda table: {}
    runner._client.load_table = lambda table, start, end, on_poll=None: {"nodes": []}

    exit_code = runner.load(_load_args("default.fresh", json_=True))

    captured = capsys.readouterr()
    assert exit_code == 0
    assert "had no snapshots when this was loaded" in captured.err
    assert "had no snapshots when this was loaded" not in captured.out
    assert json.loads(captured.out) == {"nodes": []}


def test_cmd_show_warns_when_current_range_is_empty_marked(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.fresh", None, EMPTY_TABLE_END, {"nodes": [{"id": "a", "type": "main_metadata"}]})

    exit_code = runner.show(_show_args("default.fresh"))

    err = capsys.readouterr().err
    assert exit_code == 0
    assert "had no snapshots when this was loaded" in err


def test_cmd_metadata_warns_when_current_range_is_empty_marked(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.fresh", None, EMPTY_TABLE_END, {"metadata": {"table-name": "default.fresh"}})

    exit_code = runner.metadata(_metadata_args("default.fresh"))

    err = capsys.readouterr().err
    assert exit_code == 0
    assert "had no snapshots when this was loaded" in err


def test_cmd_open_omits_end_param_and_warns_for_empty_marked_range(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.fresh", None, EMPTY_TABLE_END, {"nodes": []})

    exit_code = runner.open(_open_args("default.fresh"))

    captured = capsys.readouterr()
    out = captured.out.strip()
    assert exit_code == 0
    assert out == "http://localhost:5000/table/graph?table=default.fresh"
    assert "empty" not in out
    assert "had no snapshots when this was loaded" in captured.err


def test_cmd_use_warns_when_switching_to_empty_marked_range(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    runner._storage.save("default.fresh", None, EMPTY_TABLE_END, {"nodes": []})
    runner._storage.save("default.fresh", 100, 200, {"nodes": []})

    exit_code = runner.use(_use_args("default.fresh", index=1))

    err = capsys.readouterr().err
    assert exit_code == 0
    assert "had no snapshots when this was loaded" in err
