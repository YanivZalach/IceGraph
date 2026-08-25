# Converts encoded Iceberg file metrics into human-readable per-column values.
# Ported from these Apache Iceberg implementations:
# https://github.com/apache/iceberg/blob/7f879b11366e17a676a03f15247a821751415529/core/src/main/java/org/apache/iceberg/MetricsUtil.java
# https://github.com/apache/iceberg/blob/7f879b11366e17a676a03f15247a821751415529/core/src/main/java/org/apache/iceberg/MetadataColumns.java

from dataclasses import dataclass
from typing import Any, Dict, Optional

from iceberg_ports.iceberg_bound_decoder import decode_iceberg_bound

RawMetricMap = Optional[list[Dict[str, Any]]]
MetricMap = Optional[Dict[int, Any]]


@dataclass(frozen=True)
class RawFileMetrics:
    column_sizes: RawMetricMap
    value_counts: RawMetricMap
    null_value_counts: RawMetricMap
    nan_value_counts: RawMetricMap
    lower_bounds: RawMetricMap
    upper_bounds: RawMetricMap


@dataclass(frozen=True)
class NormalizedFileMetrics:
    column_sizes: MetricMap
    value_counts: MetricMap
    null_value_counts: MetricMap
    nan_value_counts: MetricMap
    lower_bounds: MetricMap
    upper_bounds: MetricMap


@dataclass(frozen=True)
class PrimitiveField:
    field_id: int
    qualified_name: str
    field_type: str


RESERVED_FIELDS_BY_ID = {
    2_147_483_546: PrimitiveField(2_147_483_546, "file_path", "string"),
    2_147_483_545: PrimitiveField(2_147_483_545, "pos", "long"),
}


class ReadableMetricsConverter:
    def __init__(self, current_schema_id: int, iceberg_schemas: list[Dict[str, Any]]):
        schemas_by_id = {schema["schema-id"]: schema for schema in iceberg_schemas}
        current_schema = schemas_by_id[current_schema_id]
        self._primitive_fields = sorted(self._collect_primitive_fields(current_schema), key=lambda field: field.qualified_name)
        self._historical_fields_by_id = self._collect_latest_historical_fields_by_id(iceberg_schemas)

    def convert(self, raw_metrics: RawFileMetrics) -> Dict[str, Dict[str, Any]]:
        metrics = self._normalize_metrics(raw_metrics)
        current_field_ids = {field.field_id for field in self._primitive_fields}
        metric_field_ids = self._metric_field_ids(metrics)
        current_fields = [field for field in self._primitive_fields if field.field_id in metric_field_ids]
        deprecated_fields = [self._deprecated_field(field_id) for field_id in sorted(metric_field_ids - current_field_ids)]

        return {
            field.qualified_name: {
                "source_id": field.field_id,
                "field_type": field.field_type,
                "column_size_in_bytes": self._column_size_in_bytes(metrics.column_sizes, field.field_id),
                "value_count": self._metric_value(metrics.value_counts, field.field_id),
                "null_value_count": self._metric_value(metrics.null_value_counts, field.field_id),
                "nan_value_count": self._metric_value(metrics.nan_value_counts, field.field_id),
                "lower_bound": self._decode_metric_bound(metrics.lower_bounds, field),
                "upper_bound": self._decode_metric_bound(metrics.upper_bounds, field),
            }
            for field in [*current_fields, *deprecated_fields]
        }

    def _collect_latest_historical_fields_by_id(self, iceberg_schemas: list[Dict[str, Any]]) -> Dict[int, PrimitiveField]:
        latest_fields_by_id = {}
        for schema in sorted(iceberg_schemas, key=lambda schema: schema["schema-id"], reverse=True):
            for field in self._collect_primitive_fields(schema):
                if field.field_id not in latest_fields_by_id:
                    latest_fields_by_id[field.field_id] = field

        return latest_fields_by_id

    def _normalize_metrics(self, metrics: RawFileMetrics) -> NormalizedFileMetrics:
        return NormalizedFileMetrics(
            column_sizes=self._normalize_metric_map(metrics.column_sizes),
            value_counts=self._normalize_metric_map(metrics.value_counts),
            null_value_counts=self._normalize_metric_map(metrics.null_value_counts),
            nan_value_counts=self._normalize_metric_map(metrics.nan_value_counts),
            lower_bounds=self._normalize_metric_map(metrics.lower_bounds),
            upper_bounds=self._normalize_metric_map(metrics.upper_bounds),
        )

    @staticmethod
    def _normalize_metric_map(metric_map: RawMetricMap) -> MetricMap:
        if metric_map is None:
            return None

        return {entry["key"]: entry["value"] for entry in metric_map}

    @staticmethod
    def _metric_field_ids(metrics: NormalizedFileMetrics) -> set[int]:
        metric_maps = (
            metrics.column_sizes,
            metrics.value_counts,
            metrics.null_value_counts,
            metrics.nan_value_counts,
            metrics.lower_bounds,
            metrics.upper_bounds,
        )
        field_ids = set()
        for metric_map in metric_maps:
            if metric_map:
                field_ids.update(metric_map)

        return field_ids

    def _deprecated_field(self, field_id: int) -> PrimitiveField:
        reserved_field = RESERVED_FIELDS_BY_ID.get(field_id)
        if reserved_field:
            return reserved_field

        historical_field = self._historical_fields_by_id.get(field_id)
        field_type = historical_field.field_type if historical_field else "unknown"
        field_name = f"{historical_field.qualified_name} (dropped)" if historical_field else f"__deprecated_column_id_{field_id}"
        return PrimitiveField(field_id, field_name, field_type)

    def _collect_primitive_fields(self, iceberg_schema: Dict[str, Any]) -> list[PrimitiveField]:
        fields = []
        for field in iceberg_schema.get("fields", []):
            fields.extend(self._primitive_fields_for_type(field["id"], field["name"], field["type"]))

        return fields

    def _primitive_fields_for_type(self, field_id: int, qualified_name: str, field_type: Any) -> list[PrimitiveField]:
        if isinstance(field_type, str):
            return [PrimitiveField(field_id, qualified_name, field_type)]

        type_name = field_type["type"]
        if type_name == "struct":
            fields = []
            for child_field in field_type["fields"]:
                child_name = f"{qualified_name}.{child_field['name']}"
                fields.extend(self._primitive_fields_for_type(child_field["id"], child_name, child_field["type"]))
            return fields

        if type_name == "list":
            return self._primitive_fields_for_type(field_type["element-id"], f"{qualified_name}.element", field_type["element"])

        if type_name == "map":
            key_fields = self._primitive_fields_for_type(field_type["key-id"], f"{qualified_name}.key", field_type["key"])
            value_fields = self._primitive_fields_for_type(field_type["value-id"], f"{qualified_name}.value", field_type["value"])
            return key_fields + value_fields

        return [PrimitiveField(field_id, qualified_name, "unknown")]

    @staticmethod
    def _metric_value(metric_map: MetricMap, field_id: int):
        if metric_map is None:
            return None

        return metric_map.get(field_id)

    @classmethod
    def _column_size_in_bytes(cls, column_sizes: MetricMap, field_id: int):
        column_size_bytes = cls._metric_value(column_sizes, field_id)
        if column_size_bytes is None:
            return None

        return str(column_size_bytes)

    def _decode_metric_bound(self, bounds: MetricMap, field: PrimitiveField):
        encoded_bound = self._metric_value(bounds, field.field_id)
        if encoded_bound is None:
            return None

        return decode_iceberg_bound(field.field_type, bytes(encoded_bound))
