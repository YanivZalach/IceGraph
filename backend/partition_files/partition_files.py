from datetime import date
from typing import Any, Callable

from base_classes.utils import format_snapshot_summary, timed
from collectors.collect_snapshots import SnapshotRecord
from constants import FileType
from env import Env
from graph_normalizer.utils import to_json_safe
from partition_files.constants import (
    PARTITION_FILES_LIMIT_ERROR,
    PARTITION_MATCH_UNDECIDED,
    STAGE_MATCH_DELETE_FILES,
    STAGE_PRUNE_MANIFESTS,
    STAGE_READ_MANIFEST_ENTRIES,
    STAGE_READ_TABLE_METADATA,
)
from partition_files.manifest_pruning import prune_manifests
from partition_files.partition_entries import collect_partition_entries
from partition_files.partition_filter import PartitionFilter, PartitionFilterField, is_unpartitioned_spec, resolve_partition_filter
from partition_files.partition_nodes import build_file_records, build_manifest_records, match_delete_files
from partition_files.partition_table_metadata import format_epoch_millis, read_partition_table_metadata


class PartitionFilesLimitError(ValueError):
    def __init__(self, message: str, spark_sql: str):
        super().__init__(message)
        self.spark_sql = spark_sql


class PartitionFiles:
    def __init__(self, table_name: str, snapshot_id: int, raw_filters: dict[str, str], on_stage: Callable[[str, str], None]):
        self._table_name = table_name
        self._snapshot_id = snapshot_id
        self._raw_filters = raw_filters
        self._on_stage = on_stage

    @timed
    def build(self) -> dict[str, Any]:
        with self._stage(STAGE_READ_TABLE_METADATA):
            metadata = read_partition_table_metadata(self._table_name, self._snapshot_id)
            partition_filter = resolve_partition_filter(self._raw_filters, metadata)

        with self._stage(STAGE_PRUNE_MANIFESTS):
            pruning = prune_manifests(self._table_name, metadata.snapshot["manifest-list"], partition_filter)
            if len(pruning.manifests) > Env.MAX_MANIFESTS_TO_COLLECT:
                self._raise_limit_error(partition_filter, Env.MAX_MANIFESTS_TO_COLLECT, "manifests")

        with self._stage(STAGE_READ_MANIFEST_ENTRIES):
            entries = collect_partition_entries(self._table_name, pruning.manifests, Env.MAX_PARTITION_FILES_TO_COLLECT)
            if len(entries.rows) > Env.MAX_PARTITION_FILES_TO_COLLECT:
                self._raise_limit_error(partition_filter, Env.MAX_PARTITION_FILES_TO_COLLECT, "files")

        with self._stage(STAGE_MATCH_DELETE_FILES):
            files = build_file_records(entries.rows, metadata)
            unpartitioned_spec_ids = {spec_id for spec_id, spec in metadata.specs_by_id.items() if is_unpartitioned_spec(spec)}
            match_delete_files(files, unpartitioned_spec_ids)
            manifests = build_manifest_records(pruning.manifests, files, entries.manifest_errors, metadata, self._snapshot_id)
            snapshot = self._build_snapshot_record(metadata.snapshot, [manifest.file_path for manifest in manifests])
            snapshot.errors.extend(pruning.errors)

        return to_json_safe(
            {
                "nodes": [snapshot.to_dict()] + [manifest.to_dict() for manifest in manifests] + [file.to_dict() for file in files],
                "summary": {
                    "table_name": self._table_name,
                    "snapshot_id": self._snapshot_id,
                    "snapshot_timestamp": snapshot.timestamp,
                    "format_version": metadata.format_version,
                    "filters": [self._describe_filter_field(filter_field) for filter_field in partition_filter.fields],
                    "undecided_spec_ids": sorted(
                        spec_id for spec_id, spec_filter in partition_filter.spec_filters.items() if not spec_filter.is_complete
                    ),
                    "undecided_files_count": sum(1 for file in files if file.partition_match == PARTITION_MATCH_UNDECIDED),
                },
                "errors": {},
                "warnings": {},
            }
        )

    def _stage(self, stage_name: str):
        return _Stage(self._on_stage, stage_name)

    def _build_snapshot_record(self, snapshot: dict[str, Any], manifest_paths: list[str]) -> SnapshotRecord:
        summary = {key: value for key, value in (snapshot.get("summary") or {}).items() if key != "operation"}
        operation = (snapshot.get("summary") or {}).get("operation")

        return SnapshotRecord(
            type=FileType.SNAPSHOT,
            file_path=snapshot["manifest-list"],
            timestamp=format_epoch_millis(snapshot.get("timestamp-ms")),
            snapshot_id=snapshot["snapshot-id"],
            action_link=None,
            parent_id=snapshot.get("parent-snapshot-id"),
            operation=operation,
            operation_description=operation,
            summary=format_snapshot_summary(summary),
            first_row_id=snapshot.get("first-row-id"),
            added_rows=snapshot.get("added-rows"),
            child_files=manifest_paths,
        )

    @staticmethod
    def _describe_filter_field(filter_field: PartitionFilterField) -> dict[str, Any]:
        return {
            "name": filter_field.name,
            "field_id": filter_field.field_id,
            "transform": filter_field.transform,
            "source_name": filter_field.source_name,
            "result_type": filter_field.result_type,
            "value": filter_field.raw_value,
        }

    def _raise_limit_error(self, partition_filter: PartitionFilter, limit: int, kind: str) -> None:
        message = PARTITION_FILES_LIMIT_ERROR.format(limit=limit, kind=kind, snapshot_id=self._snapshot_id)

        raise PartitionFilesLimitError(message, build_partition_files_sql(self._table_name, self._snapshot_id, partition_filter))


class _Stage:
    def __init__(self, on_stage: Callable[[str, str], None], stage_name: str):
        self._on_stage = on_stage
        self._stage_name = stage_name

    def __enter__(self):
        self._on_stage(self._stage_name, "in_progress")

    def __exit__(self, *_exc_info):
        self._on_stage(self._stage_name, "done")


def build_partition_files_sql(table_name: str, snapshot_id: int, partition_filter: PartitionFilter) -> str:
    conditions = []
    for filter_field in partition_filter.fields:
        condition = f"partition.`{filter_field.name.replace('`', '``')}` = {_sql_literal(filter_field.value)}"
        lacking_spec_ids = sorted(
            spec_id
            for spec_id, spec_filter in partition_filter.spec_filters.items()
            if not _spec_has_field(spec_filter.predicates, filter_field.field_id)
        )
        if lacking_spec_ids:
            condition = f"({condition} OR spec_id IN ({', '.join(str(spec_id) for spec_id in lacking_spec_ids)}))"
        conditions.append(condition)

    return f"SELECT * FROM {table_name}.files VERSION AS OF {snapshot_id}\nWHERE " + "\n  AND ".join(conditions)


def _spec_has_field(predicates, field_id: int) -> bool:
    for predicate in predicates:
        if predicate.filter_field.field_id == field_id:
            return True

    return False


def _sql_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"

    if isinstance(value, str):
        escaped_value = value.replace("\\", "\\\\").replace("'", "\\'")

        return f"'{escaped_value}'"

    return str(value)
