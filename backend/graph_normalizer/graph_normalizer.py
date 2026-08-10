import os

from table_inventory.table_inventory import TableInventoryResult
from graph_normalizer.utils import to_json_safe


class GraphNormalizer:

    def __init__(self, table_data: TableInventoryResult):
        self._files = table_data.metadata_files + table_data.snapshots + table_data.manifests + table_data.data_files
        self._errors = table_data.errors
        self._warnings = table_data.warnings
        self._current_table_metadata = table_data.current_table_specs

        self._path_to_nodes = {}

    def normalize(self):
        self._build_nodes()

        return to_json_safe(
            {
                "nodes": list(self._path_to_nodes.values()),
                "metadata": self._current_table_metadata,
                "errors": self._errors,
                "warnings": self._warnings,
            }
        )

    def _build_nodes(self):
        for file in self._files:
            file_path = file.file_path

            self._path_to_nodes[file_path] = {
                "id": file_path,
                "label": os.path.basename(file_path),
                "details": file.to_dict(),
                "type": file.type.value,
            }
