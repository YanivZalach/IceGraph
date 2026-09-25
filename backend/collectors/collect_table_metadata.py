import json
from contextlib import suppress
from typing import Any

from graph_cache.metadata_file import collect_graph_metadata_file
from spark_connect import open_spark_connect_session


class TableMetadataCollector:
    def __init__(self, table_name: str):
        self._table_name = table_name

    def collect_latest(self) -> dict[str, Any]:
        metadata_path = collect_graph_metadata_file(self._table_name, None)

        return self.collect(metadata_path)

    def collect(self, metadata_path: str) -> dict[str, Any]:
        spark = open_spark_connect_session()
        row = (
            spark.read.option("multiLine", True)
            .json(metadata_path)
            .drop("metadata-log")
            .drop("snapshot-log")
            .drop("snapshots")
            .drop("statistics")
            .first()
        )

        metadata = row.asDict(recursive=True)
        metadata["schemas"] = metadata.get("schemas", [])
        self._parse_schema_field_types(metadata["schemas"])

        return {"table-name": self._table_name, "metadata_file_path": metadata_path, **metadata}

    @staticmethod
    def _parse_schema_field_types(schemas: list[dict[str, Any]]) -> None:
        for schema in schemas:
            for field in schema["fields"]:
                with suppress(Exception):
                    field["type"] = json.loads(field["type"])
