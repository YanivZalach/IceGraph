import json
from typing import Any, Dict, List, Optional

from base_classes.utils import timed
from collectors.collect_metadata import POINTED_STATISTICS_SCHEMAS, CollectMetadata
from collectors.collect_table_metadata import METADATA_LOG_FIELDS, TableMetadataCollector, collect_format_version_warnings
from collectors.collector import FilesCollection
from env import Env
from icegraph_logger import logger
from trino_backend.metadata_json import json_object_with_keys, json_path, json_value, normalize_entries, read_metadata_json, snapshot_fields
from trino_backend.partition_statistics import TrinoCollectPartitionStatistics
from trino_backend.table_utils import collect_graph_metadata_file
from trino_backend.trino_client import Row, TrinoAnalysisError, metadata_table, run_query, sql_string, timestamp_bound, utc_text

INTEGER_METADATA_FIELDS = [
    "current-schema-id",
    "current-snapshot-id",
    "default-sort-order-id",
    "default-spec-id",
    "format-version",
    "last-column-id",
    "last-partition-id",
    "last-sequence-number",
    "last-updated-ms",
]
JSON_METADATA_FIELDS = ["properties", "refs", "statistics", "partition-statistics"]


class TrinoCollectMetadata(CollectMetadata):
    @timed
    def collect(self) -> FilesCollection:
        try:
            self._ordered_metadata_to_timestamp = self._query_metadata_files()
            self._apply_metadata_files_cutoff()

            snap_id_to_path = self._get_snap_id_to_path()
            for row in self._read_metadata_rows():
                self._metadata_files.append(self._parse_metadata_row(row, snap_id_to_path))

            self._metadata_files.extend(self._bad_metadata_files)
            self._apply_metadata_order()

        except Exception as e:
            logger.error(f"[{self._table_name}] metadata collection failed", exc_info=True)
            self._errors["metadata_collection"] = [str(e)]

        return FilesCollection(files=self._metadata_files, errors=self._errors, warnings=self._warnings)

    def _query_metadata_files(self) -> dict:
        rows = run_query(f"""
            SELECT file, {utc_text("timestamp")} AS metadata_timestamp
            FROM {metadata_table(self._table_name, "metadata_log_entries")}
            WHERE {timestamp_bound("timestamp", ">=", self._start_metadata_cutoff)}
                AND {timestamp_bound("timestamp", "<=", self._end_metadata_cutoff)}
            ORDER BY timestamp DESC, length(file) DESC, file DESC
            LIMIT {Env.MAX_METADATA_FILES_TO_COLLECT + 1}
        """)

        return {row.file: row.metadata_timestamp for row in rows}

    def _read_metadata_rows(self) -> List[Dict[str, Any]]:
        if not self._ordered_metadata_to_timestamp:
            return []

        metadata_read = read_metadata_json(
            self._ordered_metadata_paths,
            [
                f"{json_object_with_keys(INTEGER_METADATA_FIELDS + ['location', 'table-uuid'] + JSON_METADATA_FIELDS)} AS metadata",
                "map_keys(m) AS metadata_keys",
                f"json_extract_scalar({json_value('schema')}, {sql_string(json_path('schema-id'))}) AS legacy_schema_id",
                f"json_array_length({json_value('metadata-log')}) AS metadata_log_count",
                f"{snapshot_fields(['snapshot-id', 'manifest-list'])} AS pointed_snapshots",
            ],
        )

        rows = []
        for file, timestamp in self._ordered_metadata_to_timestamp.items():
            if file in metadata_read.errors:
                logger.error(f"[{self._table_name}] Metadata file read error for {file}: {metadata_read.errors[file]}")
                self._bad_metadata_files.append(self._build_bad_metadata_file(file, timestamp, metadata_read.errors[file]))
            else:
                rows.append(self._build_metadata_row(file, timestamp, metadata_read.rows[file]))

        return rows

    @staticmethod
    def _build_metadata_row(file: str, timestamp: str, row: Row) -> Dict[str, Any]:
        metadata = json.loads(row.metadata)
        metadata_keys = set(row.metadata_keys)
        result = {field_name: _to_int(metadata.get(field_name)) for field_name in INTEGER_METADATA_FIELDS}

        if "current-schema-id" not in metadata_keys and "schema" in metadata_keys and "schemas" not in metadata_keys:
            result["current-schema-id"] = int(row.legacy_schema_id or 0)
        if "default-spec-id" not in metadata_keys and "partition-spec" in metadata_keys and "partition-specs" not in metadata_keys:
            result["default-spec-id"] = 0

        pointed_snapshots = [
            {"snapshot-id": snapshot_id, "manifest-list": manifest_list} for snapshot_id, manifest_list in row.pointed_snapshots or []
        ]

        return {
            **result,
            "file": file,
            "metadata_timestamp": timestamp,
            "location": metadata.get("location"),
            "table-uuid": metadata.get("table-uuid"),
            "properties": json.dumps(metadata["properties"]) if metadata.get("properties") is not None else None,
            "refs": json.dumps(metadata["refs"]) if metadata.get("refs") is not None else None,
            "statistics": normalize_entries(metadata.get("statistics"), POINTED_STATISTICS_SCHEMAS["statistics"].elementType.fieldNames()),
            "partition-statistics": normalize_entries(
                metadata.get("partition-statistics"), POINTED_STATISTICS_SCHEMAS["partition-statistics"].elementType.fieldNames()
            ),
            "pointed_metadata_log_count": row.metadata_log_count or 0,
            "pointed_snapshots_files": json.dumps(pointed_snapshots) if pointed_snapshots else None,
        }


class TrinoTableMetadataCollector(TableMetadataCollector):
    PARTITION_STATISTICS_COLLECTOR = TrinoCollectPartitionStatistics

    @timed
    def collect_latest(self) -> dict[str, Any]:
        metadata_path = collect_graph_metadata_file(self._table_name, None)
        metadata = self.collect(metadata_path)

        return {**metadata, "warnings": collect_format_version_warnings(metadata)}

    def _read_metadata_row(self, metadata_path: str) -> Row:
        current_snapshot_id = f"json_extract_scalar({json_value('current-snapshot-id')}, '$')"
        snapshot_id = f"json_extract_scalar(snapshot, {sql_string(json_path('snapshot-id'))})"
        metadata_read = read_metadata_json(
            [metadata_path],
            [
                f"{json_object_with_keys(METADATA_LOG_FIELDS, exclude=True)} AS metadata",
                f"json_format(element_at(filter(CAST({json_value('snapshots')} AS array(json)), snapshot -> {snapshot_id} = {current_snapshot_id}), 1))"
                " AS current_snapshot",
                f"{snapshot_fields(['snapshot-id', 'first-row-id', 'added-rows'])} AS snapshot_row_lineage",
            ],
        )
        if metadata_path in metadata_read.errors:
            raise TrinoAnalysisError(metadata_read.errors[metadata_path])

        row = metadata_read.rows[metadata_path]
        metadata = {key: value if isinstance(value, str) else json.dumps(value) for key, value in json.loads(row.metadata).items()}
        snapshot_row_lineage = [
            Row(snapshot_id=_to_int(snapshot_id), first_row_id=_to_int(first_row_id), added_rows=_to_int(added_rows))
            for snapshot_id, first_row_id, added_rows in row.snapshot_row_lineage or []
        ]

        return Row(metadata=metadata, current_snapshot=row.current_snapshot, snapshot_row_lineage=snapshot_row_lineage)


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None

    return int(value)
