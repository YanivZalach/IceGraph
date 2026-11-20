from typing import Any, Callable, Optional

from base_classes.utils import timed
from collectors.collect_snapshots import SnapshotRecord
from env import Env
from graph_normalizer.utils import to_json_safe
from partition_files.constants import (
    PARTITION_FILES_LIMIT_ERROR,
    PARTITION_MATCH_UNDECIDED,
    STAGE_BUILD_FILE_NODES,
    STAGE_PRUNE_MANIFESTS,
    STAGE_READ_MANIFEST_ENTRIES,
    STAGE_READ_TABLE_METADATA,
)
from partition_files.manifest_pruning import ManifestPruner, ManifestPruningResult
from partition_files.partition_entries import PartitionEntries, PartitionEntriesCollector
from partition_files.partition_file_records import PartitionFileRecord, PartitionFileRecordBuilder
from partition_files.partition_files_sql import PartitionFilesSqlBuilder
from partition_files.partition_filter import PartitionFilter, PartitionFilterField, PartitionFilterResolver
from partition_files.partition_manifest_records import PartitionManifestRecord, PartitionManifestRecordBuilder
from partition_files.partition_snapshot_record import PartitionSnapshotRecordBuilder
from partition_files.partition_table_metadata import PartitionTableMetadata, PartitionTableMetadataReader


class PartitionFilesLimitError(ValueError):
    def __init__(self, message: str, spark_sql: str):
        super().__init__(message)
        self.spark_sql = spark_sql


class PartitionFiles:
    def __init__(self, table_name: str, snapshot_id: Optional[int], raw_filters: dict[str, str], on_stage: Callable[[str, str], None]):
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

        self._on_stage_start(STAGE_BUILD_FILE_NODES)
        try:
            self._build_nodes()
        finally:
            self._on_stage_end(STAGE_BUILD_FILE_NODES)

        return to_json_safe({"nodes": self._node_dicts(), "summary": self._summary(), "errors": {}, "warnings": {}})

    def _read_metadata_and_filter(self) -> None:
        self._metadata = PartitionTableMetadataReader(self._table_name, self._snapshot_id).read()
        self._snapshot_id = self._metadata.snapshot["snapshot-id"]
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
        self._files = PartitionFileRecordBuilder(self._metadata).build(self._entries.rows)
        self._manifests = PartitionManifestRecordBuilder(self._metadata, self._snapshot_id).build(
            self._pruning.manifests, self._files, self._entries.manifest_errors
        )
        self._snapshot = PartitionSnapshotRecordBuilder(self._metadata).build(self._manifests, self._pruning.errors)

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

        raise PartitionFilesLimitError(message, PartitionFilesSqlBuilder(self._table_name, self._snapshot_id, self._partition_filter).build())
