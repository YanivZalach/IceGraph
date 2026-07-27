import time
from dataclasses import dataclass

import arrow
import requests


@dataclass
class Issues:
    errors: dict
    warnings: dict


@dataclass
class GraphResult:
    nodes: list
    metadata: dict
    issues: Issues


class GraphClient:
    def __init__(self, base_url: str, **requests_kwargs):
        self.base_url = base_url
        self.requests_kwargs = requests_kwargs

    def get_graph(self, table: str, start_snapshot_id: str = None, end_snapshot_id: str = None, poll_interval_seconds: float = 1.0) -> GraphResult:
        data = self._get_graph(table, start_snapshot_id, end_snapshot_id, poll_interval_seconds)
        nodes = self._extract_node_details(data["nodes"])
        metadata = data["metadata"]
        issues = Issues(errors=data["errors"], warnings=data["warnings"])

        return GraphResult(nodes=nodes, metadata=metadata, issues=issues)

    def _extract_node_details(self, nodes: list) -> list:
        details_list = []
        for node in nodes:
            details = node.get("details", {})
            for key, value in details.items():
                if value and "timestamp" in key.lower():
                    details[key] = arrow.get(value).to("local")
            details_list.append(details)

        return details_list

    def _get_graph(self, table: str, start_snapshot_id: str, end_snapshot_id: str, poll_interval_seconds: float) -> dict:
        job_id = self._submit_graph_job(table, start_snapshot_id, end_snapshot_id)

        while True:
            result = self._get_graph_job(job_id)
            if result.get("status") != "processing":
                return result
            time.sleep(poll_interval_seconds)

    def _submit_graph_job(self, table: str, start_snapshot_id=None, end_snapshot_id=None) -> str:
        response = requests.post(
            f"{self.base_url}/api/v1/graph-data",
            data={
                "table_name": table,
                "start_snapshot_id": start_snapshot_id,
                "end_snapshot_id": end_snapshot_id,
            },
            **self.requests_kwargs,
        )
        response.raise_for_status()
        return response.json()["key"]

    def _get_graph_job(self, job_id: str) -> dict:
        response = requests.get(f"{self.base_url}/api/v1/graph-data/{job_id}", **self.requests_kwargs)
        response.raise_for_status()
        return response.json()
