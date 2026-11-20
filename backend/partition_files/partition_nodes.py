import json
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from pyspark.sql import Row

from base_classes.utils import format_snapshot_summary
from collectors.collect_data_files import CollectDataFiles, DataFileRecord, HiddenDataFileMetadata
from collectors.collect_manifests import HiddenManifestMetadata, ManifestRecord
from collectors.collect_snapshots import SnapshotRecord
from collectors.utils import format_partition
from constants import FileType
from iceberg_ports.readable_metrics import RawFileMetrics, ReadableMetricsConverter
from icegraph_logger import logger
from partition_files.constants import (
    FILE_CONTENT_DATA,
    FILE_CONTENT_EQUALITY_DELETES,
    FILE_CONTENT_POSITION_DELETES,
    PARTITION_MATCH_MATCHED,
    PARTITION_MATCH_UNDECIDED,
)
from partition_files.manifest_pruning import ManifestCandidate
from partition_files.partition_table_metadata import PartitionTableMetadata
from partition_files.time_partition_values import format_time_transform_value
from partition_files.utils import format_epoch_millis


@dataclass
class HiddenPartitionFileMetadata(HiddenDataFileMetadata):
    content: int
    partition_key: str


@dataclass
class PartitionFileRecord(DataFileRecord):
    partition_spec_id: int
    data_sequence_number: int
    partition_match: str
    delete_files: list[str] = field(default_factory=list)
    applies_to_data_files: list[str] = field(default_factory=list)


@dataclass
class PartitionManifestRecord(ManifestRecord):
    content: int
    partition_spec_id: int
    sequence_number: Optional[int]
    partition_match: str


class PartitionNodeBuilder:
    def __init__(self, metadata: PartitionTableMetadata, snapshot_id: int):
        self._metadata = metadata
        self._snapshot_id = snapshot_id
        self._converter = ReadableMetricsConverter(metadata.current_schema_id, metadata.schemas) if metadata.schemas else None

    def build_files(self, rows: list[Row]) -> list[PartitionFileRecord]:
        records_by_node_id: dict[str, PartitionFileRecord] = {}
        for row in rows:
            entry = row.asDict(recursive=True)
            record = self._build_file_record(entry)
            existing_record = records_by_node_id.get(record.node_id)
            if existing_record is None:
                records_by_node_id[record.node_id] = record
            else:
                existing_record.hidden_data_file_metadata.pointing_manifests.append({"path": entry["manifest_path"], "status": entry["status"]})

        return list(records_by_node_id.values())

    def build_manifests(
        self, manifests: list[ManifestCandidate], records: list[PartitionFileRecord], manifest_errors: dict[str, list[str]]
    ) -> list[PartitionManifestRecord]:
        records_by_manifest: dict[str, list[PartitionFileRecord]] = defaultdict(list)
        for record in records:
            for pointing_manifest in record.hidden_data_file_metadata.pointing_manifests:
                records_by_manifest[pointing_manifest["path"]].append(record)

        manifest_records = []
        for manifest in manifests:
            children = records_by_manifest.get(manifest.manifest_path, [])
            errors = manifest_errors.get(manifest.manifest_path, [])
            if not children and not errors:
                continue

            manifest_records.append(self._build_manifest_record(manifest, children, errors))

        return manifest_records

    def build_snapshot(self, manifests: list[PartitionManifestRecord], errors: list[str]) -> SnapshotRecord:
        snapshot = self._metadata.snapshot
        summary = {key: value for key, value in (snapshot.get("summary") or {}).items() if key != "operation"}
        operation = (snapshot.get("summary") or {}).get("operation")
        snapshot_record = SnapshotRecord(
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
            child_files=[manifest.file_path for manifest in manifests],
        )
        snapshot_record.errors.extend(errors)

        return snapshot_record

    def _build_manifest_record(self, manifest: ManifestCandidate, children: list[PartitionFileRecord], errors: list[str]) -> PartitionManifestRecord:
        partitions = []
        for child in children:
            if child.partition not in partitions:
                partitions.append(child.partition)

        child_ids = [child.node_id for child in children]

        return PartitionManifestRecord(
            type=FileType.MANIFEST,
            file_path=manifest.manifest_path,
            added_snapshot_id=manifest.added_snapshot_id,
            added_snapshot_timestamp=self._metadata.snapshot_timestamps.get(manifest.added_snapshot_id),
            first_row_id=None,
            first_row_id_by_snapshot=None,
            partitions=partitions,
            total_rows_in_downstream_files=sum(child.row_count or 0 for child in children),
            existing_child_files=child_ids,
            deleted_child_files=[],
            child_files=child_ids,
            content=manifest.content,
            partition_spec_id=manifest.spec_filter.spec_id,
            sequence_number=manifest.sequence_number,
            partition_match=PARTITION_MATCH_MATCHED if manifest.spec_filter.is_complete else PARTITION_MATCH_UNDECIDED,
            hidden_manifest_data=HiddenManifestMetadata(pointing_snapshots=[self._snapshot_id], row_id_assignments=[]),
            errors=list(errors),
        )

    def _build_file_record(self, entry: dict) -> PartitionFileRecord:
        content = entry["content"] or FILE_CONTENT_DATA
        added_snapshot_id = entry["snapshot_id"] if entry["snapshot_id"] is not None else entry["manifest_added_snapshot_id"]
        data_sequence_number = entry["sequence_number"] if entry["sequence_number"] is not None else entry["manifest_sequence_number"]
        raw_metrics = RawFileMetrics(
            column_sizes=entry["column_sizes"],
            value_counts=entry["value_counts"],
            null_value_counts=entry["null_value_counts"],
            nan_value_counts=entry["nan_value_counts"],
            lower_bounds=entry["lower_bounds"],
            upper_bounds=entry["upper_bounds"],
        )
        record = PartitionFileRecord(
            type=CollectDataFiles._detect_file_type(content, entry["content_offset"]),
            node_id=self._node_id(entry["file_path"], entry["content_offset"]),
            virtual_read=True,
            file_path=entry["file_path"],
            format=entry["file_format"],
            file_size_in_bytes=str(entry["file_size_in_bytes"]),
            row_count=entry["record_count"],
            first_row_id=None,
            first_row_id_by_snapshot=None,
            referenced_data_file=entry["referenced_data_file"],
            content_offset=entry["content_offset"],
            content_size_in_bytes=entry["content_size_in_bytes"],
            partition=self._format_partition(entry),
            earliest_appearing_snapshot_id=added_snapshot_id,
            earliest_appearing_snapshot_timestamp=self._metadata.snapshot_timestamps.get(added_snapshot_id),
            sort_order_id=entry["sort_order_id"],
            split_offsets=entry["split_offsets"] or [],
            key_metadata=entry["key_metadata"],
            equality_ids=json.dumps(entry["equality_ids"]) if entry["equality_ids"] is not None else None,
            readable_metrics={},
            child_files=[],
            partition_spec_id=entry["partition_spec_id"],
            data_sequence_number=data_sequence_number or 0,
            partition_match=entry["partition_match"],
            hidden_data_file_metadata=HiddenPartitionFileMetadata(
                pointing_manifests=[{"path": entry["manifest_path"], "status": entry["status"]}],
                raw_metrics=raw_metrics,
                content=content,
                partition_key=f"{entry['partition_spec_id']}:{entry['partition_json']}",
            ),
        )
        self._set_readable_metrics(record)

        return record

    def _format_partition(self, entry: dict) -> list[dict]:
        partition = format_partition(json.loads(entry["partition_json"] or "{}", parse_float=Decimal))
        spec = self._metadata.specs_by_id.get(entry["partition_spec_id"])
        transforms = {spec_field.name: spec_field.transform for spec_field in spec.fields} if spec else {}
        for partition_field in partition:
            formatted_value = format_time_transform_value(transforms.get(partition_field["name"], ""), partition_field["value"])
            if formatted_value is not None:
                partition_field["formatted_value"] = formatted_value

        return partition

    @staticmethod
    def _node_id(file_path: str, content_offset: Optional[int]) -> str:
        return file_path if content_offset is None else f"{file_path}#{content_offset}"

    def _set_readable_metrics(self, record: PartitionFileRecord) -> None:
        if self._converter is None:
            return

        try:
            record.readable_metrics = self._converter.convert(record.hidden_data_file_metadata.raw_metrics)

        except Exception as error:
            message = f"Failed to build readable metrics: {type(error).__name__}: {error}"
            logger.error(f"{message} for {record.file_path}", exc_info=True)
            record.warnings.append(message)


class DeleteFileMatcher:
    def __init__(self, records: list[PartitionFileRecord], unpartitioned_spec_ids: set[int]):
        self._records = records
        self._unpartitioned_spec_ids = unpartitioned_spec_ids
        self._data_files = [record for record in records if record.type == FileType.DATA]
        self._data_files_by_partition: dict[str, list[PartitionFileRecord]] = defaultdict(list)
        self._data_files_by_path: dict[str, PartitionFileRecord] = {}
        for data_file in self._data_files:
            self._data_files_by_partition[data_file.hidden_data_file_metadata.partition_key].append(data_file)
            self._data_files_by_path[data_file.file_path] = data_file

    def match(self) -> None:
        for delete_file in self._records:
            if delete_file.type == FileType.DATA:
                continue

            for data_file in self._affected_data_files(delete_file):
                delete_file.applies_to_data_files.append(data_file.node_id)
                data_file.delete_files.append(delete_file.node_id)

    def _affected_data_files(self, delete_file: PartitionFileRecord) -> list[PartitionFileRecord]:
        content = delete_file.hidden_data_file_metadata.content
        delete_sequence_number = delete_file.data_sequence_number
        referenced_data_file = delete_file.referenced_data_file

        if content == FILE_CONTENT_POSITION_DELETES:
            if referenced_data_file is not None:
                candidates = [self._data_files_by_path[referenced_data_file]] if referenced_data_file in self._data_files_by_path else []
            else:
                candidates = self._data_files_by_partition.get(delete_file.hidden_data_file_metadata.partition_key, [])

            return [data_file for data_file in candidates if data_file.data_sequence_number <= delete_sequence_number]

        if content == FILE_CONTENT_EQUALITY_DELETES:
            if delete_file.partition_spec_id in self._unpartitioned_spec_ids:
                candidates = self._data_files
            else:
                candidates = self._data_files_by_partition.get(delete_file.hidden_data_file_metadata.partition_key, [])

            return [data_file for data_file in candidates if data_file.data_sequence_number < delete_sequence_number]

        return []
