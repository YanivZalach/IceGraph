from datetime import date
from typing import Any, Callable, Optional

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
from partition_files.manifest_pruning import ManifestPruner, ManifestPruningResult
from partition_files.partition_entries import PartitionEntries, PartitionEntriesCollector
from partition_files.partition_filter import PartitionFilter, PartitionFilterField, PartitionFilterResolver, SpecPredicate
from partition_files.partition_nodes import DeleteFileMatcher, PartitionFileRecord, PartitionManifestRecord, PartitionNodeBuilder
from partition_files.partition_table_metadata import PartitionTableMetadata, PartitionTableMetadataReader
from partition_files.utils import format_epoch_millis


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

        self._metadata: Optional[PartitionTableMetadata] = None
        self._partition_filter: Optional[PartitionFilter] = None
        self._pruning: Optional[ManifestPruningResult] = None
        self._entries: Optional[PartitionEntries] = None

        self._files: list[PartitionFileRecord] = []
        self._manifests: list[PartitionManifestRecord] = []
        self._snapshot: Optional[SnapshotRecord] = None

    @timed
    def build(self) -> dict[str, Any]:
        self._on_stage_start(STAGE_READ_TABLE_METADATA)
        try:
            self._read_metadata_and_filter()
        finally:
            self._on_stage_end(STAGE_READ_TABLE_METADATA)

        self._on_stage_start(STAGE_PRUNE_MANIFESTS)
        try:
            self._prune_manifests()
        finally:
            self._on_stage_end(STAGE_PRUNE_MANIFESTS)

        self._on_stage_start(STAGE_READ_MANIFEST_ENTRIES)
        try:
            self._collect_entries()
        finally:
            self._on_stage_end(STAGE_READ_MANIFEST_ENTRIES)

        self._on_stage_start(STAGE_MATCH_DELETE_FILES)
        try:
            self._build_nodes()
        finally:
            self._on_stage_end(STAGE_MATCH_DELETE_FILES)

        return to_json_safe({"nodes": self._node_dicts(), "summary": self._summary(), "errors": {}, "warnings": {}})

    def _read_metadata_and_filter(self) -> None:
        self._metadata = PartitionTableMetadataReader(self._table_name, self._snapshot_id).read()
        self._partition_filter = PartitionFilterResolver(self._metadata).resolve(self._raw_filters)

    def _prune_manifests(self) -> None:
        self._pruning = ManifestPruner(self._table_name, self._metadata.snapshot["manifest-list"], self._partition_filter).prune()
        if len(self._pruning.manifests) > Env.MAX_MANIFESTS_TO_COLLECT:
            self._raise_limit_error(Env.MAX_MANIFESTS_TO_COLLECT, "manifests")

    def _collect_entries(self) -> None:
        self._entries = PartitionEntriesCollector(self._table_name, self._pruning.manifests, Env.MAX_PARTITION_FILES_TO_COLLECT).collect()
        if len(self._entries.rows) > Env.MAX_PARTITION_FILES_TO_COLLECT:
            self._raise_limit_error(Env.MAX_PARTITION_FILES_TO_COLLECT, "files")

    def _build_nodes(self) -> None:
        node_builder = PartitionNodeBuilder(self._metadata, self._snapshot_id)
        self._files = node_builder.build_files(self._entries.rows)
        unpartitioned_spec_ids = {spec_id for spec_id, spec in self._metadata.specs_by_id.items() if spec.is_unpartitioned}
        DeleteFileMatcher(self._files, unpartitioned_spec_ids).match()
        self._manifests = node_builder.build_manifests(self._pruning.manifests, self._files, self._entries.manifest_errors)
        self._snapshot = self._build_snapshot_record(self._metadata.snapshot, [manifest.file_path for manifest in self._manifests])
        self._snapshot.errors.extend(self._pruning.errors)

    def _node_dicts(self) -> list[dict[str, Any]]:
        return [self._snapshot.to_dict()] + [manifest.to_dict() for manifest in self._manifests] + [file.to_dict() for file in self._files]

    def _summary(self) -> dict[str, Any]:
        return {
            "table_name": self._table_name,
            "snapshot_id": self._snapshot_id,
            "snapshot_timestamp": self._snapshot.timestamp,
            "format_version": self._metadata.format_version,
            "filters": [self._describe_filter_field(filter_field) for filter_field in self._partition_filter.fields],
            "undecided_spec_ids": sorted(
                spec_id for spec_id, spec_filter in self._partition_filter.spec_filters.items() if not spec_filter.is_complete
            ),
            "undecided_files_count": sum(1 for file in self._files if file.partition_match == PARTITION_MATCH_UNDECIDED),
        }

    def _on_stage_start(self, stage_name: str) -> None:
        self._on_stage(stage_name, "in_progress")

    def _on_stage_end(self, stage_name: str) -> None:
        self._on_stage(stage_name, "done")

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

    def _raise_limit_error(self, limit: int, kind: str) -> None:
        message = PARTITION_FILES_LIMIT_ERROR.format(limit=limit, kind=kind, snapshot_id=self._snapshot_id)

        raise PartitionFilesLimitError(message, self._build_spark_sql(self._partition_filter))

    def _build_spark_sql(self, partition_filter: PartitionFilter) -> str:
        conditions = []
        for filter_field in partition_filter.fields:
            condition = f"partition.`{filter_field.name.replace('`', '``')}` = {self._sql_literal(filter_field.value)}"
            lacking_spec_ids = sorted(
                spec_id
                for spec_id, spec_filter in partition_filter.spec_filters.items()
                if not self._spec_has_field(spec_filter.predicates, filter_field.field_id)
            )
            if lacking_spec_ids:
                condition = f"({condition} OR spec_id IN ({', '.join(str(spec_id) for spec_id in lacking_spec_ids)}))"
            conditions.append(condition)

        return f"SELECT * FROM {self._table_name}.files VERSION AS OF {self._snapshot_id}\nWHERE " + "\n  AND ".join(conditions)

    @staticmethod
    def _spec_has_field(predicates: list[SpecPredicate], field_id: int) -> bool:
        for predicate in predicates:
            if predicate.filter_field.field_id == field_id:
                return True

        return False

    @staticmethod
    def _sql_literal(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"

        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"

        if isinstance(value, str):
            escaped_value = value.replace("\\", "\\\\").replace("'", "\\'")

            return f"'{escaped_value}'"

        return str(value)
