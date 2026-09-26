import json
from contextlib import suppress
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StringType, StructType

from base_classes.utils import collect_graph_metadata_file, format_snapshot_summary
from spark_connect import open_spark_connect_session


class TableMetadataCollector:
    def __init__(self, table_name: str):
        self._table_name = table_name

    def collect_latest(self) -> dict[str, Any]:
        metadata_path = collect_graph_metadata_file(self._table_name, None)

        return self.collect(metadata_path)

    def collect(self, metadata_path: str) -> dict[str, Any]:
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

        return {"table-name": self._table_name, "metadata_file_path": metadata_path, **metadata}

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
