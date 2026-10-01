from dataclasses import dataclass
from typing import Optional, Union

import requests
import arrow

from icegraph_client.utils.http_utils import raise_for_status
from icegraph_client.utils.time_utils import to_local_time


@dataclass
class SnapshotInfo:
    timestamp: Union[arrow.Arrow, str]
    snapshot_id: str
    operation: str


@dataclass
class SnapshotPage:
    snapshots: list[SnapshotInfo]
    next_before_snapshot_id: Optional[str]


class SnapshotsClient:
    def __init__(self, base_url: str, **requests_kwargs):
        self.base_url = base_url
        self.requests_kwargs = requests_kwargs

    def get_snapshot_map(self, table: str, before_snapshot_id: Optional[str] = None) -> SnapshotPage:
        params = {"before_snapshot_id": before_snapshot_id} if before_snapshot_id is not None else None
        response = requests.get(f"{self.base_url}/api/v1/snapshot-map/{table}", params=params, **self.requests_kwargs)
        raise_for_status(response)
        data = response.json()

        return SnapshotPage(
            snapshots=[
                SnapshotInfo(timestamp=to_local_time(timestamp, "timestamp"), snapshot_id=snapshot["snapshot_id"], operation=snapshot["operation"])
                for timestamp, snapshot in data["snapshots"].items()
            ],
            next_before_snapshot_id=data["next_before_snapshot_id"],
        )
