import os

from collectors.collect_metadata import MetadataFileRecord
from constants import FileType
from table_inventory.table_inventory import TableInventoryResult
from graph_normalizer.utils import to_json_safe


class GraphNormalizer:

    def __init__(self, table_data: TableInventoryResult):
        self._files = table_data.metadata_files + table_data.snapshots + table_data.manifests + table_data.data_files
        self._errors = table_data.errors
        self._warnings = table_data.warnings
        self._current_table_metadata = table_data.current_table_specs

        self._path_to_nodes = {}
        self._edges = []

    def normalize(self):
        self._build_nodes()
        self._build_edges()

        return to_json_safe(
            {
                "nodes": list(self._path_to_nodes.values()),
                "edges": self._edges,
                "metadata": self._current_table_metadata,
                "errors": self._errors,
                "warnings": self._warnings,
            }
        )

    def _build_nodes(self):
        for file in self._files:
            file_path = file.file_path
            color_shift = 1

            if isinstance(file, MetadataFileRecord):
                color_shift = file.hidden_metadata.color_shift

            self._path_to_nodes[file_path] = {
                "id": file_path,
                "label": os.path.basename(file_path),
                "details": file.to_dict(),
                "type": file.type.value,
                "color_shift": color_shift,
            }

    def _build_edges(self):
        for file in self._files:
            if isinstance(file, MetadataFileRecord):
                self._build_metadata_edges(file)
            else:
                self._build_generic_edges(file)

    def _build_generic_edges(self, file):
        file_path = file.file_path

        for child_file_path in file.child_files:
            if child_file_path in self._path_to_nodes and file_path in self._path_to_nodes:
                edge = {
                    "from": file_path,
                    "to": child_file_path,
                }

                if file.type == FileType.MANIFEST:
                    if child_file_path in file.deleted_child_files:
                        edge["is_deleted"] = True

                self._edges.append(edge)

    def _build_metadata_edges(self, file: MetadataFileRecord):
        main_branch_path = file.hidden_metadata.main_branch_file

        if main_branch_path in self._path_to_nodes and file.file_path in self._path_to_nodes:
            edge = {
                "from": file.file_path,
                "to": main_branch_path,
            }
            self._edges.append(edge)

        branch_files = file.hidden_metadata.branch_files

        for branch_path, branch_name in branch_files.items():
            if branch_path in self._path_to_nodes and file.file_path in self._path_to_nodes:
                edge = {
                    "from": file.file_path,
                    "to": branch_path,
                    "branch_names": branch_name,
                }
                self._edges.append(edge)
