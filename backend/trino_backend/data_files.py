from typing import List, Optional, Set

from base_classes.utils import timed
from collectors.collect_data_files import CollectDataFiles, DataFilesCutoff
from collectors.collector import FilesCollection
from env import Env
from trino_backend.manifest_files import DATA_FILE_FIELDS, METRIC_VALUE_TYPES, manifest_entries_sql, manifest_entries_table, partition_json_sql
from trino_backend.partition_layout import TableLayout, load_table_layout
from trino_backend.raw_files import MISSING_FILE_ERROR, find_existing_files
from trino_backend.row_ids import ROW_ID_NOT_FOUND_WARNING, TrinoRowIdResolver
from trino_backend.trino_client import Row, run_query, sql_string

NULL_TIMESTAMP_SORT_KEY = "''"


class TrinoCollectDataFiles(CollectDataFiles):
    @timed
    def collect(self) -> FilesCollection:
        if not self._manifests:
            return FilesCollection(files=[])

        layout = load_table_layout(self._table_name)
        entries_table = manifest_entries_table(layout, self._partition_spec_ids(layout))
        rows = run_query(self._data_files_sql(entries_table))

        cutoff = self._find_trino_cutoff(rows)
        included_rows = self._included_rows(rows, cutoff)
        if cutoff is None:
            self._report_missing_manifests(rows)

        TrinoRowIdResolver(self._table_name, self._manifests, entries_table).resolve(included_rows)
        self._data_files = [self._process_trino_row(row, entries_table.partition_type) for row in included_rows]

        if cutoff is None:
            return FilesCollection(files=self._data_files)

        warnings = {"data_files_cutoff": [self._build_cutoff_warning(cutoff)]}

        return FilesCollection(files=self._data_files, warnings=warnings, data_files_cutoff_reached=True)

    def _partition_spec_ids(self, layout: TableLayout) -> List[int]:
        spec_ids = []
        for manifest in self._manifests:
            spec_id = manifest.hidden_manifest_data.partition_spec_id
            if spec_id is None:
                return list(layout.partition_specs)

            if spec_id not in spec_ids:
                spec_ids.append(spec_id)

        return spec_ids

    def _data_files_sql(self, entries_table) -> str:
        data_file_names = list(DATA_FILE_FIELDS) + list(METRIC_VALUE_TYPES)
        data_file_types = [trino_type for trino_type, _ in DATA_FILE_FIELDS.values()]
        data_file_types.extend(f"array(row(key integer, value {trino_type}))" for trino_type, _ in METRIC_VALUE_TYPES.values())
        data_file_values = [f"data_file.{name}" for name in data_file_names]
        data_file_values[0] = "coalesce(data_file.content, 0)"
        data_file_row = (
            f"CAST(ROW({', '.join(data_file_values)}, {partition_json_sql(entries_table)}) "
            f"AS row({', '.join(f'{name} {data_type}' for name, data_type in zip(data_file_names, data_file_types))}, partition varchar))"
        )
        entries_sql = manifest_entries_sql(
            entries_table,
            [manifest.file_path for manifest in self._manifests],
            ["status", f"{data_file_row} AS data_file"],
        )
        manifest_values = ", ".join(
            f"({sql_string(manifest.file_path)}, {int(manifest.added_snapshot_id)}, "
            f"{sql_string(manifest.added_snapshot_timestamp) if manifest.added_snapshot_timestamp else 'CAST(NULL AS varchar)'})"
            for manifest in self._manifests
        )
        sort_key = f"coalesce(added_snapshot_timestamp, {NULL_TIMESTAMP_SORT_KEY})"

        return f"""
            WITH manifests (manifest_path, added_snapshot_id, added_snapshot_timestamp) AS (VALUES {manifest_values}),
            entries AS (
                SELECT
                    entry.file_path AS manifest_path,
                    entry.status,
                    entry.data_file,
                    IF(entry.data_file.content = 0, entry.data_file.first_row_id) AS stored_first_row_id,
                    IF(
                        entry.data_file.content_offset IS NULL,
                        entry.data_file.file_path,
                        entry.data_file.file_path || '#' || CAST(entry.data_file.content_offset AS varchar)
                    ) AS node_id,
                    manifests.added_snapshot_id,
                    manifests.added_snapshot_timestamp
                FROM ({entries_sql}) AS entry
                JOIN manifests ON entry.file_path = manifests.manifest_path
            )
            SELECT
                node_id,
                array_agg(CAST(ROW(manifest_path, status) AS row(path varchar, status integer))) AS pointing_manifests,
                array_agg(
                    CAST(ROW(manifest_path, status, stored_first_row_id) AS row(path varchar, status integer, stored_first_row_id bigint))
                ) AS row_id_entries,
                max(added_snapshot_timestamp) AS latest_snapshot_timestamp,
                max_by(added_snapshot_id, {sort_key}) AS latest_snapshot_id,
                min_by(added_snapshot_timestamp, {sort_key}) AS earliest_snapshot_timestamp,
                min_by(added_snapshot_id, {sort_key}) AS earliest_snapshot_id,
                max_by(data_file, {sort_key}) AS data_file
            FROM entries
            GROUP BY node_id
            ORDER BY latest_snapshot_timestamp DESC NULLS LAST, node_id
            LIMIT {Env.MAX_DATA_FILES_TO_COLLECT + 1}
        """

    @staticmethod
    def _find_trino_cutoff(rows: List[Row]) -> Optional[DataFilesCutoff]:
        if len(rows) <= Env.MAX_DATA_FILES_TO_COLLECT:
            return None

        cutoff_row = rows[Env.MAX_DATA_FILES_TO_COLLECT]

        return DataFilesCutoff(cutoff_row.latest_snapshot_id, cutoff_row.latest_snapshot_timestamp)

    @staticmethod
    def _included_rows(rows: List[Row], cutoff: Optional[DataFilesCutoff]) -> List[Row]:
        if cutoff is None:
            return rows

        cutoff_timestamp = cutoff.snapshot_timestamp or ""

        return [
            row for row in rows[: Env.MAX_DATA_FILES_TO_COLLECT] if row.latest_snapshot_timestamp and row.latest_snapshot_timestamp > cutoff_timestamp
        ]

    def _report_missing_manifests(self, rows: List[Row]) -> None:
        read_manifests: Set[str] = {pointing_manifest["path"] for row in rows for pointing_manifest in row.pointing_manifests}
        unread_manifests = {manifest.file_path: manifest for manifest in self._manifests if manifest.file_path not in read_manifests}
        existing_manifests = find_existing_files(unread_manifests)
        for manifest_path, manifest in unread_manifests.items():
            if manifest_path not in existing_manifests:
                manifest.errors.append(MISSING_FILE_ERROR.format(file_path=manifest_path))

    def _process_trino_row(self, row: Row, partition_type):
        data_file = row.data_file
        data_file_dict = {
            **data_file,
            "node_id": row.node_id,
            "partition": partition_type.to_python(data_file["partition"]),
            "pointing_manifests": row.pointing_manifests,
            "row_id_entries": [{"row_id_offset": None, **entry} for entry in row.row_id_entries],
            "earliest_snapshot_id": row.earliest_snapshot_id,
            "earliest_snapshot_timestamp": row.earliest_snapshot_timestamp,
        }
        data_file_record = self._process_data_file_row(Row(data_file_dict))
        if row.get("row_id_not_found"):
            data_file_record.warnings.append(ROW_ID_NOT_FOUND_WARNING)

        return data_file_record
