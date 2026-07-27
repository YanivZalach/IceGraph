from dataclasses import dataclass

import requests
import arrow


@dataclass
class SnapshotInfo:
    timestamp: arrow.Arrow
    snapshot_id: str
    operation: str


class SnapshotsClient:
    def __init__(self, base_url: str, **requests_kwargs):
        self.base_url = base_url
        self.requests_kwargs = requests_kwargs

    def get_snapshot_map(self, table: str) -> list[SnapshotInfo]:
        response = requests.get(f"{self.base_url}/api/v1/snapshot-map/{table}", **self.requests_kwargs)
        response.raise_for_status()
        data = response.json()

        return [
            SnapshotInfo(timestamp=arrow.get(timestamp).to("local"), snapshot_id=snapshot["snapshot_id"], operation=snapshot["operation"])
            for timestamp, snapshot in data.items()
        ]
