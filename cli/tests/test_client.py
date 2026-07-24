from unittest.mock import MagicMock

import pytest

from icegraph_client.client import IcegraphClient, IcegraphError, JobFailedError


def make_response(status_code, json_data=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    if json_data is None:
        resp.json.side_effect = ValueError("no json body")
    else:
        resp.json.return_value = json_data
    return resp


def test_list_tables_success():
    session = MagicMock()
    session.get.return_value = make_response(200, {"tables": ["default.events"], "include_none_iceberg_catalogs": True})
    client = IcegraphClient("http://localhost:5000", session=session)

    result = client.list_tables()

    assert result.tables == ["default.events"]
    assert result.include_none_iceberg_catalogs is True
    session.get.assert_called_once_with("http://localhost:5000/api/v1/tables")


def test_list_tables_error():
    session = MagicMock()
    session.get.return_value = make_response(500, {"error": "boom"})
    client = IcegraphClient("http://localhost:5000", session=session)

    with pytest.raises(IcegraphError, match="boom"):
        client.list_tables()


def test_server_url_trailing_slash_is_stripped():
    client = IcegraphClient("http://localhost:5000/", session=MagicMock())
    assert client.server_url == "http://localhost:5000"


def test_submit_graph_job_builds_form_and_returns_key():
    session = MagicMock()
    session.post.return_value = make_response(202, {"key": "default_logging_1_2", "status": "processing"})
    client = IcegraphClient("http://localhost:5000", session=session)

    job_id = client.submit_graph_job("default.logging", start_snapshot_id=1, end_snapshot_id=2)

    assert job_id == "default_logging_1_2"
    session.post.assert_called_once_with(
        "http://localhost:5000/api/v1/graph-data",
        data={"table_name": "default.logging", "start_snapshot_id": "1", "end_snapshot_id": "2"},
    )


def test_submit_graph_job_omits_missing_range():
    session = MagicMock()
    session.post.return_value = make_response(202, {"key": "default_logging_None_None", "status": "processing"})
    client = IcegraphClient("http://localhost:5000", session=session)

    client.submit_graph_job("default.logging")

    session.post.assert_called_once_with(
        "http://localhost:5000/api/v1/graph-data",
        data={"table_name": "default.logging"},
    )


def test_poll_job_returns_status_and_body():
    session = MagicMock()
    session.get.return_value = make_response(200, {"nodes": []})
    client = IcegraphClient("http://localhost:5000", session=session)

    status_code, data = client.poll_job("some-job")

    assert status_code == 200
    assert data == {"nodes": []}


def test_load_table_polls_until_complete():
    session = MagicMock()
    session.post.return_value = make_response(202, {"key": "job-1", "status": "processing"})
    session.get.side_effect = [
        make_response(202, {"key": "job-1", "status": "processing"}),
        make_response(200, {"nodes": [{"id": "a"}]}),
    ]
    client = IcegraphClient("http://localhost:5000", session=session)

    result = client.load_table("default.logging", poll_interval=0)

    assert result == {"nodes": [{"id": "a"}]}
    assert session.get.call_count == 2


def test_load_table_raises_on_failure():
    session = MagicMock()
    session.post.return_value = make_response(202, {"key": "job-1", "status": "processing"})
    session.get.return_value = make_response(400, {"error": "table not found"})
    client = IcegraphClient("http://localhost:5000", session=session)

    with pytest.raises(JobFailedError, match="table not found"):
        client.load_table("default.logging", poll_interval=0)


def test_snapshot_map_success():
    session = MagicMock()
    session.get.return_value = make_response(200, {"2026-01-01T00:00:00+00:00": {"snapshot_id": "1", "operation": "append"}})
    client = IcegraphClient("http://localhost:5000", session=session)

    result = client.snapshot_map("default.logging")

    assert result["2026-01-01T00:00:00+00:00"]["operation"] == "append"
    session.get.assert_called_once_with("http://localhost:5000/api/v1/snapshot-map/default.logging")
