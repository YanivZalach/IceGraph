import json
from contextlib import suppress
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType, StructType

from base_classes.utils import collect_graph_metadata_file, format_snapshot_summary, timed
from collectors.collect_partition_statistics import CollectPartitionStatistics, PartitionStatisticsFileRecord
from graph_normalizer.utils import to_json_safe
from spark_connect import open_spark_connect_session


class TableMetadataCollector:
    def __init__(self, table_name: str):
        self._table_name = table_name

    @timed
    def collect_latest(self) -> dict[str, Any]:
        metadata_path = collect_graph_metadata_file(self._table_name, None)

        return self.collect(metadata_path)

    def collect(self, metadata_path: str, partition_statistics_files: list[PartitionStatisticsFileRecord] | None = None) -> dict[str, Any]:
        spark = open_spark_connect_session()
        metadata_df = spark.read.option("multiLine", True).json(metadata_path)
        row = (
            metadata_df.withColumn("current-snapshot", self._current_snapshot_column(metadata_df))
            .drop("metadata-log")
            .drop("snapshot-log")
            .drop("snapshots")
            .drop("statistics")
            .first()
        )

        metadata = row.asDict(recursive=True)
        metadata["schemas"] = metadata.get("schemas", [])
        self._parse_schema_field_types(metadata["schemas"])
        self._format_current_snapshot_summary(metadata["current-snapshot"])
        self._collect_current_partition_statistics(metadata, metadata_path, partition_statistics_files or [])

        return to_json_safe({"table-name": self._table_name, "metadata_file_path": metadata_path, **metadata})

    def _collect_current_partition_statistics(
        self, metadata: dict[str, Any], metadata_path: str, partition_statistics_files: list[PartitionStatisticsFileRecord]
    ) -> None:
        snapshot_id = metadata.get("current-snapshot-id")
        if snapshot_id is None or snapshot_id == -1:
            return

        entry = next((entry for entry in metadata.get("partition-statistics") or [] if entry["snapshot-id"] == snapshot_id), None)
        if entry is None:
            return

        statistics_file = next((file for file in partition_statistics_files if file.file_path == entry["statistics-path"]), None)
        if statistics_file is None:
            statistics_file = CollectPartitionStatistics(self._table_name, []).collect_file(metadata_path, entry, include_samples=False)

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

    @staticmethod
    def _current_snapshot_column(metadata_df: DataFrame) -> Column:
        snapshots_type = metadata_df.schema["snapshots"].dataType if "snapshots" in metadata_df.columns else None
        if not (isinstance(snapshots_type, ArrayType) and isinstance(snapshots_type.elementType, StructType)):
            return F.lit(None)

        current_snapshots = F.filter("snapshots", lambda snapshot: snapshot["snapshot-id"] == F.col("current-snapshot-id"))
        current_snapshot = F.get(current_snapshots, 0)
        summary_json = F.to_json(current_snapshot["summary"], {"ignoreNullFields": "true"})
        summary_without_absent_keys = F.from_json(summary_json, MapType(StringType(), StringType()))

        return current_snapshot.withField("summary", summary_without_absent_keys)

    @staticmethod
    def _parse_schema_field_types(schemas: list[dict[str, Any]]) -> None:
        for schema in schemas:
            for field in schema["fields"]:
                with suppress(Exception):
                    field["type"] = json.loads(field["type"])

    @staticmethod
    def _format_current_snapshot_summary(current_snapshot: dict[str, Any] | None) -> None:
        if current_snapshot:
            current_snapshot["summary"] = format_snapshot_summary(current_snapshot["summary"] or {})
