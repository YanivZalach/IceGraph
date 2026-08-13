from typing import Callable, Dict, Optional

from base_classes.base_file import BaseFile
from table_inventory.table_inventory import TableInventoryResult
from graph_normalizer.utils import to_json_safe


class GraphNormalizer:

    def __init__(self, table_data: TableInventoryResult):
        self._files = table_data.metadata_files + table_data.snapshots + table_data.manifests + table_data.data_files
        self._table_errors = table_data.errors
        self._table_warnings = table_data.warnings
        self._current_table_metadata = table_data.current_table_specs

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
