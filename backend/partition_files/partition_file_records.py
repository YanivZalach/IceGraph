import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from pyspark.sql import Row

from collectors.collect_data_files import CollectDataFiles, DataFileRecord, HiddenDataFileMetadata
from collectors.utils import format_partition
from iceberg_ports.readable_metrics import RawFileMetrics, ReadableMetricsConverter
from icegraph_logger import logger
from partition_files.constants import FILE_CONTENT_DATA
from partition_files.partition_table_metadata import PartitionTableMetadata
from partition_files.time_partition_utils import format_time_transform_value


@dataclass
class PartitionFileRecord(DataFileRecord):
    partition_spec_id: int
    partition_match: str


class PartitionFileRecordBuilder:
    def __init__(self, metadata: PartitionTableMetadata):
        self._metadata = metadata
        self._converter = ReadableMetricsConverter(metadata.current_schema_id, metadata.schemas) if metadata.schemas else None

    def build(self, rows: list[Row]) -> list[PartitionFileRecord]:
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

    def _build_file_record(self, entry: dict) -> PartitionFileRecord:
        content = entry["content"] or FILE_CONTENT_DATA
        added_snapshot_id = entry["snapshot_id"] if entry["snapshot_id"] is not None else entry["manifest_added_snapshot_id"]
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
            first_row_id=self._first_row_id(entry),
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
            partition_match=entry["partition_match"],
            hidden_data_file_metadata=HiddenDataFileMetadata(
                pointing_manifests=[{"path": entry["manifest_path"], "status": entry["status"]}],
                raw_metrics=raw_metrics,
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
    def _first_row_id(entry: dict) -> Optional[int]:
        if entry["stored_first_row_id"] is not None:
            return entry["stored_first_row_id"]

        if entry["manifest_first_row_id"] is None or entry["row_id_offset"] is None:
            return None

        return entry["manifest_first_row_id"] + entry["row_id_offset"]

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
