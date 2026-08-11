from dataclasses import dataclass
from typing import Union

import requests
import arrow

from icegraph_client.utils.http_utils import raise_for_status
from icegraph_client.utils.time_utils import to_local_time


@dataclass
class SnapshotInfo:
    timestamp: Union[arrow.Arrow, str]
    snapshot_id: str
    operation: str


class SnapshotsClient:
    def __init__(self, base_url: str, **requests_kwargs):
        self.base_url = base_url
        self.requests_kwargs = requests_kwargs

    def get_snapshot_map(self, table: str) -> list[SnapshotInfo]:
        response = requests.get(f"{self.base_url}/api/v1/snapshot-map/{table}", **self.requests_kwargs)
        raise_for_status(response)
        data = response.json()

        return [
            SnapshotInfo(timestamp=to_local_time(timestamp, "timestamp"), snapshot_id=snapshot["snapshot_id"], operation=snapshot["operation"])
            for timestamp, snapshot in data.items()
        ]
