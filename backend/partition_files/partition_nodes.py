import json
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from pyspark.sql import Row

from collectors.collect_data_files import CollectDataFiles, DataFileRecord, HiddenDataFileMetadata
from collectors.collect_manifests import HiddenManifestMetadata, ManifestRecord
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


def build_file_records(rows: list[Row], metadata: PartitionTableMetadata) -> list[PartitionFileRecord]:
    converter = ReadableMetricsConverter(metadata.current_schema_id, metadata.schemas) if metadata.schemas else None
    records_by_node_id: dict[str, PartitionFileRecord] = {}
    for row in rows:
        entry = row.asDict(recursive=True)
        record = _build_file_record(entry, metadata, converter)
        existing_record = records_by_node_id.get(record.node_id)
        if existing_record is None:
            records_by_node_id[record.node_id] = record
        else:
            existing_record.hidden_data_file_metadata.pointing_manifests.append({"path": entry["manifest_path"], "status": entry["status"]})

    return list(records_by_node_id.values())


def _build_file_record(entry: dict, metadata: PartitionTableMetadata, converter: Optional[ReadableMetricsConverter]) -> PartitionFileRecord:
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
        node_id=_node_id(entry["file_path"], entry["content_offset"]),
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
        partition=_format_partition(entry, metadata),
        earliest_appearing_snapshot_id=added_snapshot_id,
        earliest_appearing_snapshot_timestamp=metadata.snapshot_timestamps.get(added_snapshot_id),
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
    _set_readable_metrics(record, converter)

    return record


def _format_partition(entry: dict, metadata: PartitionTableMetadata) -> list[dict]:
    partition = format_partition(json.loads(entry["partition_json"] or "{}", parse_float=Decimal))
    spec = metadata.specs_by_id.get(entry["partition_spec_id"])
    transforms = {spec_field.name: spec_field.transform for spec_field in spec.fields} if spec else {}
    for partition_field in partition:
        formatted_value = format_time_transform_value(transforms.get(partition_field["name"], ""), partition_field["value"])
        if formatted_value is not None:
            partition_field["formatted_value"] = formatted_value

    return partition


def _node_id(file_path: str, content_offset: Optional[int]) -> str:
    return file_path if content_offset is None else f"{file_path}#{content_offset}"


def _set_readable_metrics(record: PartitionFileRecord, converter: Optional[ReadableMetricsConverter]) -> None:
    if converter is None:
        return

    try:
        record.readable_metrics = converter.convert(record.hidden_data_file_metadata.raw_metrics)

    except Exception as error:
        message = f"Failed to build readable metrics: {type(error).__name__}: {error}"
        logger.error(f"{message} for {record.file_path}", exc_info=True)
        record.warnings.append(message)


def match_delete_files(records: list[PartitionFileRecord], unpartitioned_spec_ids: set[int]) -> None:
    data_files = [record for record in records if record.type == FileType.DATA]
    data_files_by_partition: dict[str, list[PartitionFileRecord]] = defaultdict(list)
    data_files_by_path: dict[str, PartitionFileRecord] = {}
    for data_file in data_files:
        data_files_by_partition[data_file.hidden_data_file_metadata.partition_key].append(data_file)
        data_files_by_path[data_file.file_path] = data_file

    for delete_file in records:
        if delete_file.type == FileType.DATA:
            continue

        for data_file in _affected_data_files(delete_file, data_files, data_files_by_partition, data_files_by_path, unpartitioned_spec_ids):
            delete_file.applies_to_data_files.append(data_file.node_id)
            data_file.delete_files.append(delete_file.node_id)


def _affected_data_files(
    delete_file: PartitionFileRecord,
    data_files: list[PartitionFileRecord],
    data_files_by_partition: dict[str, list[PartitionFileRecord]],
    data_files_by_path: dict[str, PartitionFileRecord],
    unpartitioned_spec_ids: set[int],
) -> list[PartitionFileRecord]:
    content = delete_file.hidden_data_file_metadata.content
    delete_sequence_number = delete_file.data_sequence_number

    if content == FILE_CONTENT_POSITION_DELETES:
        if delete_file.referenced_data_file is not None:
            candidates = [data_files_by_path[delete_file.referenced_data_file]] if delete_file.referenced_data_file in data_files_by_path else []
        else:
            candidates = data_files_by_partition.get(delete_file.hidden_data_file_metadata.partition_key, [])

        return [data_file for data_file in candidates if data_file.data_sequence_number <= delete_sequence_number]

    if content == FILE_CONTENT_EQUALITY_DELETES:
        if delete_file.partition_spec_id in unpartitioned_spec_ids:
            candidates = data_files
        else:
            candidates = data_files_by_partition.get(delete_file.hidden_data_file_metadata.partition_key, [])

        return [data_file for data_file in candidates if data_file.data_sequence_number < delete_sequence_number]

    return []


def build_manifest_records(
    manifests: list[ManifestCandidate],
    records: list[PartitionFileRecord],
    manifest_errors: dict[str, list[str]],
    metadata: PartitionTableMetadata,
    snapshot_id: int,
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

        partitions = []
        for child in children:
            if child.partition not in partitions:
                partitions.append(child.partition)

        child_ids = [child.node_id for child in children]
        manifest_records.append(
            PartitionManifestRecord(
                type=FileType.MANIFEST,
                file_path=manifest.manifest_path,
                added_snapshot_id=manifest.added_snapshot_id,
                added_snapshot_timestamp=metadata.snapshot_timestamps.get(manifest.added_snapshot_id),
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
                hidden_manifest_data=HiddenManifestMetadata(pointing_snapshots=[snapshot_id], row_id_assignments=[]),
                errors=list(errors),
            )
        )

    return manifest_records
