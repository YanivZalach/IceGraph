import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

from partition_files.constants import (
    EMPTY_PARTITION_FILTER_ERROR,
    INVALID_PARTITION_VALUE_ERROR,
    UNKNOWN_PARTITION_FIELD_ERROR,
    UNSUPPORTED_PARTITION_TYPE_ERROR,
    VOID_TRANSFORM,
)
from partition_files.partition_table_metadata import PartitionSpec, PartitionSpecField, PartitionTableMetadata
from partition_files.time_partition_utils import TIME_TRANSFORM_FORMATS, is_time_transform, parse_time_transform_value

INTEGER_RESULT_TRANSFORMS = {"year", "month", "hour"}
DECIMAL_TYPE_PATTERN = r"decimal\(\s*\d+\s*,\s*\d+\s*\)"


@dataclass(frozen=True)
class PartitionFilterField:
    name: str
    field_id: int
    transform: str
    source_name: Optional[str]
    result_type: str
    raw_value: str
    value: Any


@dataclass(frozen=True)
class SpecPredicate:
    filter_field: PartitionFilterField
    spec_field_name: str
    spec_field_position: int


@dataclass(frozen=True)
class SpecFilter:
    spec_id: int
    predicates: list[SpecPredicate]
    is_complete: bool


@dataclass(frozen=True)
class PartitionFilter:
    fields: list[PartitionFilterField]
    spec_filters: dict[int, SpecFilter]


class PartitionFilterResolver:
    def __init__(self, metadata: PartitionTableMetadata):
        self._metadata = metadata

    def resolve(self, raw_filters: dict[str, str]) -> PartitionFilter:
        if not raw_filters:
            raise ValueError(EMPTY_PARTITION_FILTER_ERROR)

        fields = [self._resolve_filter_field(name, raw_value) for name, raw_value in raw_filters.items()]
        spec_filters = {spec_id: self._build_spec_filter(spec, fields) for spec_id, spec in self._metadata.specs_by_id.items()}

        return PartitionFilter(fields=fields, spec_filters=spec_filters)

    def _resolve_filter_field(self, name: str, raw_value: str) -> PartitionFilterField:
        spec_field = self._find_spec_field_by_name(name)
        source = self._find_schema_field(spec_field.source_id)
        source_type = source.get("type") if source else None
        result_type = self._result_type(spec_field.transform, source_type)

        return PartitionFilterField(
            name=name,
            field_id=spec_field.field_id,
            transform=spec_field.transform,
            source_name=source.get("name") if source else None,
            result_type=result_type,
            raw_value=raw_value,
            value=self._parse_transform_value(name, raw_value, spec_field.transform, result_type),
        )

    def _find_spec_field_by_name(self, name: str) -> PartitionSpecField:
        for spec_id in sorted(self._metadata.specs_by_id, reverse=True):
            for spec_field in self._metadata.specs_by_id[spec_id].fields:
                if spec_field.name == name and spec_field.transform != VOID_TRANSFORM:
                    return spec_field

        raise ValueError(UNKNOWN_PARTITION_FIELD_ERROR.format(name=name))

    def _find_schema_field(self, source_id: Optional[int]) -> Optional[dict[str, Any]]:
        if source_id is None:
            return None

        ordered_schemas = sorted(self._metadata.schemas, key=lambda schema: schema.get("schema-id") != self._metadata.current_schema_id)
        for schema in ordered_schemas:
            schema_field = self._find_nested_field(schema.get("fields") or [], source_id)
            if schema_field is not None:
                return schema_field

        return None

    @staticmethod
    def _find_nested_field(fields: list[dict[str, Any]], field_id: int) -> Optional[dict[str, Any]]:
        for schema_field in fields:
            if schema_field.get("id") == field_id:
                return schema_field

            field_type = schema_field.get("type")
            if isinstance(field_type, dict) and field_type.get("type") == "struct":
                nested_field = PartitionFilterResolver._find_nested_field(field_type.get("fields") or [], field_id)
                if nested_field is not None:
                    return nested_field

        return None

    @staticmethod
    def _result_type(transform: str, source_type: Any) -> str:
        if transform.startswith("bucket") or transform in INTEGER_RESULT_TRANSFORMS:
            return "int"

        if transform == "day":
            return "date"

        if isinstance(source_type, str):
            return source_type

        return "unknown"

    @staticmethod
    def _parse_transform_value(name: str, raw_value: str, transform: str, result_type: str) -> Any:
        if not is_time_transform(transform):
            return PartitionFilterResolver._parse_value(name, raw_value, result_type)

        try:
            return parse_time_transform_value(transform, raw_value)

        except ValueError as error:
            field_type = f"{transform} value ({TIME_TRANSFORM_FORMATS[transform]})"
            raise ValueError(INVALID_PARTITION_VALUE_ERROR.format(value=raw_value, field_type=field_type, name=name)) from error

    @staticmethod
    def _parse_value(name: str, raw_value: str, result_type: str) -> Any:
        if result_type not in {"int", "long", "date", "string", "boolean"} and not re.fullmatch(DECIMAL_TYPE_PATTERN, result_type):
            raise ValueError(UNSUPPORTED_PARTITION_TYPE_ERROR.format(name=name, field_type=result_type))

        try:
            return PartitionFilterResolver._convert_value(raw_value, result_type)

        except (ValueError, InvalidOperation) as error:
            raise ValueError(INVALID_PARTITION_VALUE_ERROR.format(value=raw_value, field_type=result_type, name=name)) from error

    @staticmethod
    def _convert_value(raw_value: str, result_type: str) -> Any:
        if result_type in {"int", "long"}:
            if not re.fullmatch(r"-?\d+", raw_value.strip()):
                raise ValueError(raw_value)
            value = int(raw_value)
            bits = 32 if result_type == "int" else 64
            if not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                raise ValueError(raw_value)

            return value

        if result_type == "date":
            return date.fromisoformat(raw_value.strip())

        if result_type == "boolean":
            if raw_value.strip().lower() not in {"true", "false"}:
                raise ValueError(raw_value)

            return raw_value.strip().lower() == "true"

        if result_type == "string":
            return raw_value

        value = Decimal(raw_value.strip())
        if not value.is_finite():
            raise ValueError(raw_value)

        return value

    @staticmethod
    def _build_spec_filter(spec: PartitionSpec, fields: list[PartitionFilterField]) -> SpecFilter:
        predicates = []
        for filter_field in fields:
            for position, spec_field in enumerate(spec.fields):
                if spec_field.field_id == filter_field.field_id and spec_field.transform != VOID_TRANSFORM:
                    predicates.append(SpecPredicate(filter_field, spec_field.name, position))
                    break

        return SpecFilter(
            spec_id=spec.spec_id,
            predicates=predicates,
            is_complete=len(predicates) == len(fields),
        )
