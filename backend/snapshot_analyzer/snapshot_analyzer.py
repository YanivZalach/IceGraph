from constants import REPLACE_OPERATION
from snapshot_analyzer.utils import get_replace_operation_from_summary
from table_inventory.table_inventory import TableInventoryResult


class SnapshotAnalyzer:
    def __init__(self, table_data: TableInventoryResult):
        self._table_data = table_data

    def analyze(self) -> TableInventoryResult:
        for snapshot in self._table_data.snapshots:
            if snapshot.operation == REPLACE_OPERATION:
                snapshot.operation = get_replace_operation_from_summary(snapshot.summary)

        return self._table_data
