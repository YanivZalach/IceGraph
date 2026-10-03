from table_inventory.table_inventory import TableInventoryResult
from graph_normalizer.utils import to_json_safe


class GraphNormalizer:
    def __init__(self, table_data: TableInventoryResult):
        self._files = (
            table_data.metadata_files
            + self._order_statistics_files_by_metadata_file(table_data)
            + table_data.snapshots
            + table_data.manifests
            + table_data.data_files
        )
        self._table_errors = table_data.errors
        self._table_warnings = table_data.warnings
        self._current_table_metadata = table_data.current_table_specs

    @staticmethod
    def _order_statistics_files_by_metadata_file(table_data: TableInventoryResult) -> list:
        metadata_file_order = {metadata_file.file_path: index for index, metadata_file in enumerate(table_data.metadata_files)}

        return sorted(
            table_data.table_statistics_files + table_data.partition_statistics_files,
            key=lambda statistics_file: metadata_file_order[statistics_file.hidden_statistics_data.added_by_metadata_file],
        )

    def normalize(self):
        nodes = [file.to_dict() for file in self._files]

        return to_json_safe(
            {
                "nodes": nodes,
                "metadata": self._current_table_metadata,
                "errors": self._table_errors,
                "warnings": self._table_warnings,
            }
        )
