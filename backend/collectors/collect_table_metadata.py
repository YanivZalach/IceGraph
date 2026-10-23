import json
from dataclasses import dataclass
from typing import Any, Optional

from pyspark.sql import Column, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType

from base_classes.utils import collect_graph_metadata_file, format_snapshot_summary, timed
from collectors.collect_partition_statistics import CollectPartitionStatistics, PartitionStatisticsFileRecord
from collectors.utils import normalize_legacy_v1_metadata_dict
from constants import SUPPORTED_FORMAT_VERSIONS, UNSUPPORTED_FORMAT_VERSION_WARNING
from graph_normalizer.utils import to_json_safe
from spark_connect import open_spark_connect_session


def collect_format_version_warnings(metadata: dict[str, Any]) -> dict[str, list[str]]:
    format_version = metadata.get("format-version")
    if format_version is None or format_version in SUPPORTED_FORMAT_VERSIONS:
        return {}

    supported_format_versions = [str(version) for version in SUPPORTED_FORMAT_VERSIONS]
    supported_format_versions_text = f"{', '.join(supported_format_versions[:-1])}, and {supported_format_versions[-1]}"

    return {
        "unsupported_format_version": [
            UNSUPPORTED_FORMAT_VERSION_WARNING.format(format_version=format_version, supported_format_versions=supported_format_versions_text)
        ]
    }


METADATA_LOG_FIELDS = ["snapshots", "metadata-log", "snapshot-log"]
STRING_METADATA_FIELDS = {"location", "table-uuid"}


@dataclass
class SnapshotRowLineage:
    first_row_id: Optional[int]
    added_rows: Optional[int]


class TableMetadataCollector:
    def __init__(self, table_name: str):
        self._table_name = table_name
        self.snapshot_row_lineage_by_id: dict[int, SnapshotRowLineage] = {}

    @timed
    def collect_latest(self) -> dict[str, Any]:
        metadata_path = collect_graph_metadata_file(self._table_name, None)
        metadata = self.collect(metadata_path)

        return {**metadata, "warnings": collect_format_version_warnings(metadata)}

    def collect(self, metadata_path: str, partition_statistics_files: list[PartitionStatisticsFileRecord] | None = None) -> dict[str, Any]:
        metadata_row = self._read_metadata_row(metadata_path)
        self.snapshot_row_lineage_by_id = self._build_snapshot_row_lineage_by_id(metadata_row)

        metadata = {key: self._parse_metadata_value(key, value) for key, value in metadata_row.metadata.items()}
        normalize_legacy_v1_metadata_dict(metadata)
        metadata.setdefault("schemas", [])
        metadata["current-snapshot"] = json.loads(metadata_row.current_snapshot) if metadata_row.current_snapshot else None
        self._format_current_snapshot_summary(metadata["current-snapshot"])
        self._collect_current_partition_statistics(metadata, metadata_path, partition_statistics_files or [])

        return to_json_safe({"table-name": self._table_name, "metadata_file_path": metadata_path, **metadata})

    def _collect_current_partition_statistics(
        self, metadata: dict[str, Any], metadata_path: str, partition_statistics_files: list[PartitionStatisticsFileRecord]
    ) -> None:
        statistics_file = self._collect_current_partition_statistics_file(metadata, metadata_path, partition_statistics_files)
        if statistics_file is None:
            return

        statistics = statistics_file.to_dict()
        metadata["current-partition-statistics"] = {
            key: statistics[key]
            for key in (
                "file_path",
                "snapshot_id",
                "file_size_in_bytes",
                "partitions_count",
                "partitions_with_deletes",
                "partition_distribution",
                "errors",
                "warnings",
            )
        }

    def _collect_current_partition_statistics_file(
        self, metadata: dict[str, Any], metadata_path: str, partition_statistics_files: list[PartitionStatisticsFileRecord]
    ) -> PartitionStatisticsFileRecord | None:
        snapshot_id = metadata.get("current-snapshot-id")
        if snapshot_id is None or snapshot_id == -1:
            return None

        entry = None
        for candidate in metadata.get("partition-statistics") or []:
            if candidate["snapshot-id"] == snapshot_id:
                entry = candidate
                break
        if entry is None:
            return None

        statistics_file = None
        for candidate in partition_statistics_files:
            if candidate.file_path == entry["statistics-path"]:
                statistics_file = candidate
                break
        if statistics_file is None:
            statistics_file = CollectPartitionStatistics(self._table_name, []).collect_file(metadata_path, entry, include_samples=False)

        return statistics_file

    def _read_metadata_row(self, metadata_path: str) -> Row:
        metadata_without_logs = F.map_filter("metadata", lambda key, value: ~key.isin(METADATA_LOG_FIELDS))
        current_snapshot = F.get(F.filter("snapshots", self._is_current_snapshot), 0)
        snapshot_row_lineage = F.transform("snapshots", self._snapshot_row_lineage_column)

        metadata_row_df = self._metadata_df(metadata_path).select(
            metadata_without_logs.alias("metadata"),
            current_snapshot.alias("current_snapshot"),
            snapshot_row_lineage.alias("snapshot_row_lineage"),
        )

        return metadata_row_df.first()

    @staticmethod
    def _metadata_df(metadata_path: str) -> DataFrame:
        metadata_text_df = open_spark_connect_session().read.text(metadata_path, wholetext=True)
        parsed_metadata_df = metadata_text_df.select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))

        return parsed_metadata_df.select(
            "metadata",
            F.from_json(F.col("metadata")["snapshots"], ArrayType(StringType())).alias("snapshots"),
        )

    @staticmethod
    def _is_current_snapshot(snapshot: Column) -> Column:
        return F.get_json_object(snapshot, "$['snapshot-id']") == F.col("metadata")["current-snapshot-id"]

    @staticmethod
    def _snapshot_row_lineage_column(snapshot: Column) -> Column:
        return F.struct(
            F.get_json_object(snapshot, "$['snapshot-id']").cast(LongType()).alias("snapshot_id"),
            F.get_json_object(snapshot, "$['first-row-id']").cast(LongType()).alias("first_row_id"),
            F.get_json_object(snapshot, "$['added-rows']").cast(LongType()).alias("added_rows"),
        )

    @staticmethod
    def _build_snapshot_row_lineage_by_id(metadata_row: Row) -> dict[int, SnapshotRowLineage]:
        return {
            snapshot.snapshot_id: SnapshotRowLineage(snapshot.first_row_id, snapshot.added_rows)
            for snapshot in metadata_row.snapshot_row_lineage or []
        }

    @staticmethod
    def _parse_metadata_value(key: str, value: str | None) -> Any:
        if value is None or key in STRING_METADATA_FIELDS:
            return value

        try:
            return json.loads(value)

        except ValueError:
            return value

    @staticmethod
    def _format_current_snapshot_summary(current_snapshot: dict[str, Any] | None) -> None:
        if current_snapshot:
            current_snapshot["summary"] = format_snapshot_summary(current_snapshot.get("summary") or {})
