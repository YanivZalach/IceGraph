from dataclasses import dataclass
from typing import Callable, Dict, List, Set

from collectors.collect_manifests import ManifestRecord
from icegraph_logger import logger
from trino_backend.manifest_files import MANIFEST_LIST_FILES, ManifestEntriesTable, manifest_entries_sql
from trino_backend.raw_files import PATHS_PER_QUERY, path_batches, read_raw_files
from trino_backend.trino_client import Row, metadata_table, qualified_table, run_query, sql_strings, utc_text

MANIFEST_ENTRY_STATUS_DELETED = 2
ROW_ID_NOT_FOUND_WARNING = "Its row ID could not be found in the table's metadata or rows, so it is not shown."


@dataclass(frozen=True)
class RowLineageAnchor:
    manifest_first_row_id: int
    record_count: int

    def matches(self, row: Row) -> bool:
        return (
            row.first_row_id is not None
            and row.last_row_id is not None
            and row.live_rows == self.record_count
            and row.last_row_id - row.first_row_id + 1 == self.record_count
            and row.first_row_id >= self.manifest_first_row_id
        )


class TrinoRowIdResolver:
    def __init__(self, table_name: str, manifests: List[ManifestRecord], entries_table: ManifestEntriesTable):
        self._table_name = table_name
        self._manifests = manifests
        self._entries_table = entries_table
        self._assignments_by_manifest = {manifest.file_path: manifest.hidden_manifest_data.row_id_assignments for manifest in manifests}

    def resolve(self, data_file_rows: List[Row]) -> None:
        inheriting_rows = [row for row in data_file_rows if self._inheriting_entries(row)]
        if not inheriting_rows:
            return

        row_ids = {row.node_id: self._stored_row_id(row) for row in inheriting_rows}
        unresolved = {node_id for node_id, row_id in row_ids.items() if row_id is None}
        for find_row_ids in (self._current_row_ids, self._later_stored_row_ids):
            if unresolved:
                row_ids.update(self._safely_find_row_ids(find_row_ids, unresolved))
                unresolved = {node_id for node_id in unresolved if row_ids.get(node_id) is None}
        if unresolved:
            row_ids.update(self._row_lineage_row_ids([row for row in inheriting_rows if row.node_id in unresolved]))

        for row in inheriting_rows:
            row_id = row_ids.get(row.node_id)
            if row_id is None:
                row["row_id_not_found"] = True
                continue

            for entry in self._inheriting_entries(row):
                entry["row_id_offset"] = row_id - self._assignments_by_manifest[entry["path"]][0].first_row_id

    def _inheriting_entries(self, row: Row) -> List[Dict]:
        if row.data_file["content"] != 0:
            return []

        return [
            entry
            for entry in row.row_id_entries
            if entry["stored_first_row_id"] is None
            and entry["status"] != MANIFEST_ENTRY_STATUS_DELETED
            and self._assignments_by_manifest.get(entry["path"])
        ]

    @staticmethod
    def _stored_row_id(row: Row):
        for entry in row.row_id_entries:
            if entry["stored_first_row_id"] is not None:
                return entry["stored_first_row_id"]

        return None

    def _safely_find_row_ids(self, find_row_ids: Callable[[Set[str]], Dict[str, int]], file_paths: Set[str]) -> Dict[str, int]:
        try:
            return find_row_ids(file_paths)

        except Exception:
            logger.warning(f"[{self._table_name}] Row ID lookup {find_row_ids.__name__} failed", exc_info=True)

            return {}

    def _current_row_ids(self, file_paths: Set[str]) -> Dict[str, int]:
        row_ids = {}
        for batch in path_batches(file_paths, PATHS_PER_QUERY):
            rows = run_query(f"""
                SELECT file_path, first_row_id
                FROM {metadata_table(self._table_name, "files")}
                WHERE content = 0 AND first_row_id IS NOT NULL AND file_path IN ({sql_strings(batch)})
            """)
            row_ids.update({row.file_path: row.first_row_id for row in rows})

        return row_ids

    def _later_stored_row_ids(self, file_paths: Set[str]) -> Dict[str, int]:
        later_manifest_paths = self._later_manifest_paths()
        logger.info(f"[{self._table_name}] Reading {len(later_manifest_paths)} later manifests to find {len(file_paths)} row IDs")
        if not later_manifest_paths:
            return {}

        row_ids = {}
        select = ["data_file.file_path AS data_file_path", "data_file.first_row_id AS first_row_id"]
        for manifest_batch in path_batches(later_manifest_paths, PATHS_PER_QUERY):
            entries_sql = manifest_entries_sql(self._entries_table, manifest_batch, select)
            for batch in path_batches(file_paths, PATHS_PER_QUERY):
                rows = run_query(f"""
                    SELECT data_file_path, max(first_row_id) AS first_row_id
                    FROM ({entries_sql})
                    WHERE first_row_id IS NOT NULL AND data_file_path IN ({sql_strings(batch)})
                    GROUP BY data_file_path
                """)
                row_ids.update({row.data_file_path: row.first_row_id for row in rows})

        return row_ids

    def _row_lineage_row_ids(self, rows: List[Row]) -> Dict[str, int]:
        anchors_by_snapshot: Dict[int, Dict[str, RowLineageAnchor]] = {}
        for row in rows:
            assignment = self._assignments_by_manifest[self._inheriting_entries(row)[0]["path"]][0]
            anchor = RowLineageAnchor(assignment.first_row_id, row.data_file["record_count"])
            anchors_by_snapshot.setdefault(assignment.snapshot_id, {})[row.node_id] = anchor

        row_ids = {}
        for snapshot_id, anchors in anchors_by_snapshot.items():
            for batch in path_batches(anchors, PATHS_PER_QUERY):
                try:
                    lineage_rows = run_query(f"""
                        SELECT "$path" AS file_path, min("$row_id") AS first_row_id, max("$row_id") AS last_row_id, count(*) AS live_rows
                        FROM {qualified_table(self._table_name)} FOR VERSION AS OF {int(snapshot_id)}
                        WHERE "$path" IN ({sql_strings(batch)})
                        GROUP BY "$path"
                    """)

                except Exception:
                    logger.warning(f"[{self._table_name}] Failed to read row IDs of snapshot {snapshot_id}", exc_info=True)
                    continue

                row_ids.update({row.file_path: row.first_row_id for row in lineage_rows if anchors[row.file_path].matches(row)})

        return row_ids

    def _later_manifest_paths(self) -> Set[str]:
        range_snapshot_ids = {snapshot_id for manifest in self._manifests for snapshot_id in manifest.hidden_manifest_data.pointing_snapshots}
        snapshot_rows = run_query(f"""
            SELECT snapshot_id, {utc_text("committed_at")} AS committed_at, manifest_list
            FROM {metadata_table(self._table_name, "snapshots")}
        """)
        range_start = min((row.committed_at for row in snapshot_rows if row.snapshot_id in range_snapshot_ids), default="")
        later_snapshots = {
            row.manifest_list: row.snapshot_id
            for row in snapshot_rows
            if row.snapshot_id not in range_snapshot_ids and row.committed_at > range_start and row.manifest_list
        }
        if not later_snapshots:
            return set()

        read_manifest_paths = {manifest.file_path for manifest in self._manifests}
        manifest_list_rows = read_raw_files(MANIFEST_LIST_FILES, later_snapshots, "manifest_path, added_snapshot_id")
        later_snapshot_ids = set(later_snapshots.values())

        return {
            row.manifest_path
            for row in manifest_list_rows
            if row.added_snapshot_id in later_snapshot_ids and row.manifest_path not in read_manifest_paths
        }
