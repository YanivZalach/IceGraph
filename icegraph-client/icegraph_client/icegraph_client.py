from urllib.parse import urlparse

from icegraph_client.tables_client import TablesClient
from icegraph_client.snapshots_client import SnapshotsClient
from icegraph_client.graph_client import GraphClient


class IceGraphClient:
    def __init__(self, base_url: str, **requests_kwargs):
        if "://" not in base_url:
            base_url = f"https://{base_url}"

        parsed_base_url = urlparse(base_url)
        self._base_url = f"{parsed_base_url.scheme}://{parsed_base_url.netloc}"

        self._tables_client = TablesClient(self._base_url, **requests_kwargs)
        self._snapshots_client = SnapshotsClient(self._base_url, **requests_kwargs)
        self._graph_client = GraphClient(self._base_url, **requests_kwargs)

    def list_tables(self):
        return self._tables_client.list_tables()

    def get_snapshot_map(self, table: str):
        return self._snapshots_client.get_snapshot_map(table)

    def get_graph(self, table: str, start_snapshot_id: str = None, end_snapshot_id: str = None, poll_interval_seconds: float = 1.0):
        if start_snapshot_id is None and end_snapshot_id is None:
            snapshots = self.get_snapshot_map(table)

            if snapshots:
                latest_snapshot = max(snapshots, key=lambda snapshot: snapshot.timestamp)
                print(
                    f"No snapshot range given, selected only the latest snapshot: {latest_snapshot.snapshot_id} at date {latest_snapshot.timestamp.humanize()}"
                )
                start_snapshot_id = latest_snapshot.snapshot_id

            else:
                print(f"Table {table} has no data, both start and end snapshot remain unset")

        return self._graph_client.get_graph(table, start_snapshot_id, end_snapshot_id, poll_interval_seconds)
