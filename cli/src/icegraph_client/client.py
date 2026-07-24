from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import requests

DEFAULT_SERVER_URL = "http://localhost:5000"
POLL_INTERVAL_SECONDS = 1.0


class IcegraphError(Exception):
    pass


class JobFailedError(IcegraphError):
    pass


@dataclass
class TablesResponse:
    tables: list
    include_none_iceberg_catalogs: bool


class IcegraphClient:
    def __init__(self, server_url: str = DEFAULT_SERVER_URL, session: Optional[requests.Session] = None):
        self.server_url = server_url.rstrip("/")
        self._session = session or requests.Session()

    def list_tables(self) -> TablesResponse:
        resp = self._session.get(f"{self.server_url}/api/v1/tables")
        data = self._json_or_raise(resp)
        return TablesResponse(tables=data["tables"], include_none_iceberg_catalogs=data["include_none_iceberg_catalogs"])

    def submit_graph_job(self, table_name: str, start_snapshot_id: Optional[int] = None, end_snapshot_id: Optional[int] = None) -> str:
        form = {"table_name": table_name}
        if start_snapshot_id is not None:
            form["start_snapshot_id"] = str(start_snapshot_id)
        if end_snapshot_id is not None:
            form["end_snapshot_id"] = str(end_snapshot_id)

        resp = self._session.post(f"{self.server_url}/api/v1/graph-data", data=form)
        data = self._json_or_raise(resp)
        return data["key"]

    def poll_job(self, job_id: str) -> tuple:
        resp = self._session.get(f"{self.server_url}/api/v1/graph-data/{job_id}")
        return resp.status_code, self._safe_json(resp) or {}

    def load_table(
        self,
        table_name: str,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
        poll_interval: float = POLL_INTERVAL_SECONDS,
    ) -> dict:
        job_id = self.submit_graph_job(table_name, start_snapshot_id, end_snapshot_id)

        while True:
            status_code, data = self.poll_job(job_id)
            if status_code == 200:
                return data
            if status_code == 202:
                time.sleep(poll_interval)
                continue
            raise JobFailedError(data.get("error", f"Job failed with status {status_code}"))

    def snapshot_map(self, table_name: str) -> dict:
        resp = self._session.get(f"{self.server_url}/api/v1/snapshot-map/{table_name}")
        return self._json_or_raise(resp)

    @staticmethod
    def _safe_json(resp: requests.Response) -> Optional[dict]:
        try:
            return resp.json()
        except ValueError:
            return None

    def _json_or_raise(self, resp: requests.Response) -> dict:
        data = self._safe_json(resp)
        if resp.status_code >= 400:
            message = data.get("error") if data else resp.text
            raise IcegraphError(message)
        return data
