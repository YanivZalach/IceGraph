import json
import time
from dataclasses import dataclass

import requests

from icegraph_client.clients.graph_client import JOB_TOKEN_HEADER, Issues
from icegraph_client.utils.http_utils import raise_for_status
from icegraph_client.utils.time_utils import to_local_time

SPARK_SQL_FIELD = "spark_sql"


@dataclass
class SubmittedJob:
    key: str
    token: str


@dataclass
class PartitionFilesResult:
    nodes: list
    summary: dict
    issues: Issues


class PartitionFilesClient:
    def __init__(self, base_url: str, **requests_kwargs):
        self.base_url = base_url
        self.requests_kwargs = requests_kwargs

    def get_partition_files(self, table: str, snapshot_id: str, filters: dict[str, str], poll_interval_seconds: float = 1.0) -> PartitionFilesResult:
        job = self._submit_job(table, snapshot_id, filters)

        while True:
            data = self._get_job(job)
            if data.get("status") != "processing":
                break
            time.sleep(poll_interval_seconds)

        nodes = data["nodes"]
        summary = data["summary"]
        self._localize_timestamps(nodes)
        self._localize_timestamps([summary])

        return PartitionFilesResult(nodes=nodes, summary=summary, issues=Issues(errors=data["errors"], warnings=data["warnings"]))

    @staticmethod
    def _localize_timestamps(items: list):
        for item in items:
            for key, value in item.items():
                if value and "timestamp" in key.lower():
                    item[key] = to_local_time(value, key)

    def _submit_job(self, table: str, snapshot_id: str, filters: dict[str, str]) -> SubmittedJob:
        response = requests.post(
            f"{self.base_url}/api/v1/partition-files",
            data={"table_name": table, "snapshot_id": snapshot_id, "filters": json.dumps(filters)},
            **self.requests_kwargs,
        )
        raise_for_status(response)
        body = response.json()

        return SubmittedJob(key=body["key"], token=body[JOB_TOKEN_HEADER])

    def _get_job(self, job: SubmittedJob) -> dict:
        kwargs = {**self.requests_kwargs}
        kwargs["headers"] = {**kwargs.get("headers", {}), JOB_TOKEN_HEADER: job.token}
        response = requests.get(f"{self.base_url}/api/v1/partition-files/{job.key}", **kwargs)
        self._raise_with_spark_sql(response)

        return response.json()

    @staticmethod
    def _raise_with_spark_sql(response: requests.Response) -> None:
        try:
            raise_for_status(response)

        except requests.HTTPError as e:
            try:
                spark_sql = response.json().get(SPARK_SQL_FIELD)

            except ValueError:
                spark_sql = None

            if not spark_sql:
                raise

            raise requests.HTTPError(f"{e}\n{spark_sql}", response=response) from e
