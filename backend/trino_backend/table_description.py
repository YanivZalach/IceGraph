import json
from typing import Any, Dict, List, Optional

from base_classes.utils import timed
from collectors.collect_table_description import COLUMNS_SECTION_TITLE
from collectors.utils import normalize_legacy_v1_metadata_dict
from trino_backend.metadata_json import json_text, read_metadata_json
from trino_backend.table_utils import ICEBERG_CONNECTOR_NAME, collect_graph_metadata_file
from trino_backend.trino_client import TrinoAnalysisError, parse_table_name, qualified_table, run_query, sql_identifier, sql_string

METADATA_KEYS = [
    "schemas",
    "schema",
    "current-schema-id",
    "partition-specs",
    "partition-spec",
    "default-spec-id",
    "properties",
    "location",
    "table-uuid",
    "format-version",
    "current-snapshot-id",
]
DETAILS_SECTION_TITLE = "Detailed Table Information"
OWNER_PROPERTY = "owner"
DEFAULT_FILE_FORMAT = "parquet"
SPARK_TRANSFORM_NAMES = {"year": "years", "month": "months", "day": "days", "hour": "hours"}


class TrinoTableDescriptionCollector:
    def __init__(self, full_table_name: str):
        self._table_name = full_table_name

    @timed
    def collect(self) -> dict[str, Any]:
        if self._connector_name() != ICEBERG_CONNECTOR_NAME:
            return self._describe_columns()

        metadata_path = collect_graph_metadata_file(self._table_name, None)
        metadata = self._read_metadata(metadata_path)
        schema = self._current_schema(metadata)
        field_names = _field_names(schema)
        partition_fields = self._current_partition_fields(metadata)

        return {
            "spark_schema": schema,
            "spark_partitions": [
                {"name": f"Part {index}", "value": _partition_expression(partition_field, field_names), "comment": ""}
                for index, partition_field in enumerate(partition_fields)
            ],
            "sections": [{"title": DETAILS_SECTION_TITLE, "rows": self._detail_rows(metadata, metadata_path)}],
            "properties": _table_properties(metadata),
            "warnings": {},
        }

    def _connector_name(self) -> Optional[str]:
        rows = run_query(
            f"SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = {sql_string(parse_table_name(self._table_name).catalog)}"
        )

        return rows[0].connector_name if rows else None

    def _describe_columns(self) -> dict[str, Any]:
        rows = run_query(f"DESCRIBE {qualified_table(self._table_name)}")
        column_rows = [{"name": row.Column, "value": row.Type, "comment": row.Comment or ""} for row in rows]

        return {
            "spark_schema": None,
            "spark_partitions": [],
            "sections": [{"title": COLUMNS_SECTION_TITLE, "rows": column_rows}],
            "properties": None,
            "warnings": {},
        }

    def _read_metadata(self, metadata_path: str) -> Dict[str, Any]:
        metadata_read = read_metadata_json([metadata_path], [f"{json_text(key)} AS {sql_identifier(key)}" for key in METADATA_KEYS])
        if metadata_path in metadata_read.errors:
            raise TrinoAnalysisError(metadata_read.errors[metadata_path])

        row = metadata_read.rows[metadata_path]
        metadata = {key: json.loads(row[key]) for key in METADATA_KEYS if row[key] is not None}
        normalize_legacy_v1_metadata_dict(metadata)

        return metadata

    @staticmethod
    def _current_schema(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        current_schema = None
        for schema in metadata.get("schemas") or []:
            if schema.get("schema-id") == metadata.get("current-schema-id"):
                current_schema = schema
                break

        return current_schema

    @staticmethod
    def _current_partition_fields(metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        partition_fields = []
        for spec in metadata.get("partition-specs") or []:
            if spec.get("spec-id") == metadata.get("default-spec-id"):
                partition_fields = spec.get("fields") or []
                break

        return partition_fields

    def _detail_rows(self, metadata: Dict[str, Any], metadata_path: str) -> List[Dict[str, str]]:
        details = {
            "Name": self._table_name,
            "Location": metadata.get("location"),
            "Provider": ICEBERG_CONNECTOR_NAME,
            "Owner": (metadata.get("properties") or {}).get(OWNER_PROPERTY),
            "Table UUID": metadata.get("table-uuid"),
            "Format Version": metadata.get("format-version"),
            "Current Snapshot ID": metadata.get("current-snapshot-id"),
            "Metadata File": metadata_path,
        }

        return [{"name": name, "value": "" if value is None else str(value), "comment": ""} for name, value in details.items()]


def _field_names(schema: Optional[Dict[str, Any]], prefix: str = "") -> Dict[int, str]:
    field_names: Dict[int, str] = {}
    if not isinstance(schema, dict) or schema.get("type") != "struct":
        return field_names

    for schema_field in schema.get("fields", []):
        qualified_name = f"{prefix}{schema_field['name']}"
        field_names[schema_field["id"]] = qualified_name
        field_names.update(_field_names(schema_field["type"], f"{qualified_name}."))

    return field_names


def _partition_expression(partition_field: Dict[str, Any], field_names: Dict[int, str]) -> str:
    source_name = field_names.get(partition_field["source-id"], str(partition_field["source-id"]))
    transform = partition_field["transform"]
    if transform == "identity":
        return source_name

    if "[" in transform:
        transform_name, argument = transform.rstrip("]").split("[", 1)

        return f"{transform_name}({argument}, {source_name})"

    return f"{SPARK_TRANSFORM_NAMES.get(transform, transform)}({source_name})"


def _table_properties(metadata: Dict[str, Any]) -> Dict[str, str]:
    properties = metadata.get("properties") or {}
    current_snapshot_id = metadata.get("current-snapshot-id")
    table_properties = {
        "current-snapshot-id": "none" if current_snapshot_id in (None, -1) else str(current_snapshot_id),
        "format": f"iceberg/{properties.get('write.format.default', DEFAULT_FILE_FORMAT)}",
        "format-version": str(metadata.get("format-version", 1)),
    }
    table_properties.update({key: value for key, value in properties.items() if key != OWNER_PROPERTY})

    return table_properties
