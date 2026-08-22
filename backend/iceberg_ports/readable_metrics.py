# Ported from Apache Iceberg's MetricsUtil and Conversions implementations:
# https://github.com/apache/iceberg/blob/7f879b11366e17a676a03f15247a821751415529/core/src/main/java/org/apache/iceberg/MetricsUtil.java
# https://github.com/apache/iceberg/blob/7f879b11366e17a676a03f15247a821751415529/api/src/main/java/org/apache/iceberg/types/Conversions.java
# https://github.com/apache/iceberg/blob/7f879b11366e17a676a03f15247a821751415529/core/src/main/java/org/apache/iceberg/MetadataColumns.java

import base64
import re
import struct
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Optional
from uuid import UUID

RawMetricMap = Optional[Dict[int, Any] | list[Dict[str, Any]]]
BYTES_PER_MIB = 1024 * 1024


@dataclass(frozen=True)
class RawFileMetrics:
    column_sizes: RawMetricMap
    value_counts: RawMetricMap
    null_value_counts: RawMetricMap
    nan_value_counts: RawMetricMap
    lower_bounds: RawMetricMap
    upper_bounds: RawMetricMap


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
    def __init__(self, iceberg_schema: Dict[str, Any], historical_schemas: Optional[list[Dict[str, Any]]] = None):
        self._primitive_fields = sorted(self._collect_primitive_fields(iceberg_schema), key=lambda field: field.qualified_name)
        self._historical_fields_by_id = {
            field.field_id: field
            for schema in sorted(historical_schemas or [], key=lambda schema: int(schema.get("schema-id", 0)))
            for field in self._collect_primitive_fields(schema)
        }

    def convert(self, metrics: RawFileMetrics) -> Dict[str, Dict[str, Any]]:
        column_sizes = self._normalize_metric_map(metrics.column_sizes)
        value_counts = self._normalize_metric_map(metrics.value_counts)
        null_value_counts = self._normalize_metric_map(metrics.null_value_counts)
        nan_value_counts = self._normalize_metric_map(metrics.nan_value_counts)
        lower_bounds = self._normalize_metric_map(metrics.lower_bounds)
        upper_bounds = self._normalize_metric_map(metrics.upper_bounds)

        metric_maps = (
            column_sizes,
            value_counts,
            null_value_counts,
            nan_value_counts,
            lower_bounds,
            upper_bounds,
        )
        current_field_ids = {field.field_id for field in self._primitive_fields}
        metric_field_ids = {field_id for metric_map in metric_maps if metric_map for field_id in metric_map}
        deprecated_fields = [self._deprecated_field(field_id) for field_id in sorted(metric_field_ids - current_field_ids)]

        return {
            field.qualified_name: {
                "source_id": field.field_id,
                "field_type": field.field_type,
                "column_size_mib": self._column_size_mib(column_sizes, field.field_id),
                "value_count": self._metric_value(value_counts, field.field_id),
                "null_value_count": self._metric_value(null_value_counts, field.field_id),
                "nan_value_count": self._metric_value(nan_value_counts, field.field_id),
                "lower_bound": self._decode_metric_bound(lower_bounds, field),
                "upper_bound": self._decode_metric_bound(upper_bounds, field),
            }
            for field in [*self._primitive_fields, *deprecated_fields]
        }

    def _deprecated_field(self, field_id: int) -> PrimitiveField:
        reserved_field = RESERVED_FIELDS_BY_ID.get(field_id)
        if reserved_field:
            return reserved_field

        historical_field = self._historical_fields_by_id.get(field_id)
        field_type = historical_field.field_type if historical_field else "unknown"
        return PrimitiveField(field_id, f"__deprecated_column_id_{field_id}", field_type)

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
    def _normalize_metric_map(metric_map: RawMetricMap) -> Optional[Dict[int, Any]]:
        if metric_map is None or isinstance(metric_map, dict):
            return metric_map

        return {entry["key"]: entry["value"] for entry in metric_map}

    @staticmethod
    def _metric_value(metric_map: Optional[Dict[int, Any]], field_id: int):
        if metric_map is None:
            return None

        return metric_map.get(field_id)

    @classmethod
    def _column_size_mib(cls, column_sizes: Optional[Dict[int, Any]], field_id: int):
        column_size_bytes = cls._metric_value(column_sizes, field_id)
        if column_size_bytes is None:
            return None

        return column_size_bytes / BYTES_PER_MIB

    def _decode_metric_bound(self, bounds: Optional[Dict[int, bytearray]], field: PrimitiveField):
        encoded_bound = self._metric_value(bounds, field.field_id)
        if encoded_bound is None:
            return None

        return self._decode_bound(field.field_type, bytes(encoded_bound))

    def _decode_bound(self, field_type: str, encoded_bound: bytes):
        if field_type == "boolean":
            return encoded_bound[0] != 0

        if field_type in {"int", "date"}:
            value = struct.unpack("<i", encoded_bound)[0]
            return self._decode_date(value) if field_type == "date" else value

        if field_type in {"long", "time", "timestamp", "timestamptz", "timestamp_ns", "timestamptz_ns"}:
            value = self._decode_promoted_long(encoded_bound)
            return self._decode_long_value(field_type, value)

        if field_type == "float":
            return struct.unpack("<f", encoded_bound)[0]

        if field_type == "double":
            return struct.unpack("<d", encoded_bound)[0] if len(encoded_bound) >= 8 else float(struct.unpack("<f", encoded_bound)[0])

        if field_type == "string":
            return encoded_bound.decode("utf-8")

        if field_type == "uuid":
            return str(UUID(bytes=encoded_bound))

        if field_type == "binary" or field_type.startswith("fixed["):
            return base64.b64encode(encoded_bound).decode("ascii")

        decimal_match = re.fullmatch(r"decimal\(\s*\d+\s*,\s*(\d+)\s*\)", field_type)
        if decimal_match:
            scale = int(decimal_match.group(1))
            unscaled_value = int.from_bytes(encoded_bound, byteorder="big", signed=True)
            return str(Decimal(unscaled_value).scaleb(-scale))

        if field_type == "unknown":
            return None

        raise ValueError(f"Unsupported Iceberg primitive type: {field_type}")

    @staticmethod
    def _decode_promoted_long(encoded_bound: bytes) -> int:
        if len(encoded_bound) < 8:
            return struct.unpack("<i", encoded_bound)[0]

        return struct.unpack("<q", encoded_bound)[0]

    def _decode_long_value(self, field_type: str, value: int):
        if field_type == "long":
            return value

        if field_type == "time":
            return self._decode_time(value)

        if field_type in {"timestamp", "timestamptz"}:
            return self._decode_timestamp(value)

        return self._decode_nanosecond_timestamp(value)

    @staticmethod
    def _decode_date(days_since_epoch: int) -> str:
        return (date(1970, 1, 1) + timedelta(days=days_since_epoch)).isoformat()

    @staticmethod
    def _decode_time(microseconds_since_midnight: int) -> str:
        hours, remainder = divmod(microseconds_since_midnight, 3_600_000_000)
        minutes, remainder = divmod(remainder, 60_000_000)
        seconds, microseconds = divmod(remainder, 1_000_000)
        return time(hours, minutes, seconds, microseconds).isoformat()

    @staticmethod
    def _decode_timestamp(microseconds_since_epoch: int) -> str:
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        timestamp = epoch + timedelta(microseconds=microseconds_since_epoch)
        return timestamp.isoformat()

    @staticmethod
    def _decode_nanosecond_timestamp(nanoseconds_since_epoch: int) -> str:
        seconds, nanoseconds = divmod(nanoseconds_since_epoch, 1_000_000_000)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        timestamp = epoch + timedelta(seconds=seconds)
        return f"{timestamp:%Y-%m-%dT%H:%M:%S}.{nanoseconds:09d}+00:00"
