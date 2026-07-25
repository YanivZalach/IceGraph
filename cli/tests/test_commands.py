import argparse
import json

from icegraph_client.commands.commands import CommandRunner
from icegraph_client.config.config import CliConfig


def _show_args(table, start=None, end=None, type_=None, operation=None, issues=False, node=None, children=None, json_=False):
    return argparse.Namespace(
        table=table,
        start=start,
        end=end,
        type=type_,
        operation=operation,
        issues=issues,
        node=node,
        children=children,
        json=json_,
    )


def _open_args(table, start=None, end=None, page="graph", no_browser=True):
    return argparse.Namespace(table=table, start=start, end=end, page=page, no_browser=no_browser)


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


def test_cmd_show_json_without_filters_dumps_full_result(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    result = {"nodes": [{"id": "a", "type": "metadata"}], "edges": [], "metadata": {"schema": {}}, "errors": {}, "warnings": {}}
    runner._storage.save("default.logging", None, None, result)

    exit_code = runner.show(_show_args("default.logging", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == result


def test_cmd_show_json_with_type_filter_dumps_only_nodes(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))
    nodes = [{"id": "a", "type": "metadata"}, {"id": "b", "type": "snapshot"}]
    runner._storage.save("default.logging", None, None, {"nodes": nodes, "edges": [], "metadata": {}})

    exit_code = runner.show(_show_args("default.logging", type_="snapshot", json_=True))

    out = capsys.readouterr().out
    assert exit_code == 0
    assert json.loads(out) == [{"id": "b", "type": "snapshot"}]


def test_cmd_open_prints_url_with_range(tmp_path, capsys):
    runner = CommandRunner(CliConfig(server_url="http://localhost:5000", data_dir=tmp_path))

    exit_code = runner.open(_open_args("default.logging", start=1, end=2))

    out = capsys.readouterr().out.strip()
    assert exit_code == 0
    assert out == "http://localhost:5000/table/graph?table=default.logging&start_snapshot_id=1&end_snapshot_id=2"


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
