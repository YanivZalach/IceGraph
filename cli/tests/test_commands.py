import argparse

from icegraph_client.commands import CommandRunner
from icegraph_client.config import CliConfig


def _show_args(table, start=None, end=None, type_=None, operation=None):
    return argparse.Namespace(table=table, start=start, end=end, type=type_, operation=operation)


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
