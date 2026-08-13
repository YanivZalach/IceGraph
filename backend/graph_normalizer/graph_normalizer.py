from table_inventory.table_inventory import TableInventoryResult
from graph_normalizer.utils import to_json_safe


class GraphNormalizer:

    def __init__(self, table_data: TableInventoryResult):
        self._files = table_data.metadata_files + table_data.snapshots + table_data.manifests + table_data.data_files
        self._errors = table_data.errors
        self._warnings = table_data.warnings
        self._current_table_metadata = table_data.current_table_specs

    def normalize(self):
        self._attach_errors_to_files()

        nodes = [file.to_dict() for file in self._files]

        return to_json_safe(
            {
                "nodes": nodes,
                "metadata": self._current_table_metadata,
                "errors": self._errors,
                "warnings": self._warnings,
            }
        )

    def _attach_errors_to_files(self):
        for file in self._files:
            file.error = self._errors.get(file.file_path)
