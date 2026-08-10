from constants import REPLACE_OPERATION
from snapshot_analyzer.utils import describe_replace
from table_inventory.table_inventory import TableInventoryResult


class SnapshotAnalyzer:
    def __init__(self, table_data: TableInventoryResult):
        self._table_data = table_data

    def analyze(self) -> TableInventoryResult:
        for snapshot in self._table_data.snapshots:
            if snapshot.operation == REPLACE_OPERATION:
                snapshot.operation = describe_replace(snapshot.summary)

        return self._table_data
