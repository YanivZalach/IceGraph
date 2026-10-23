import math
import unicodedata
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import arrow

from iceberg_ports.date_time_decoder import decode_nanosecond_timestamp, decode_timestamp


V1_PARTITION_FIELD_ID_START = 1000
TIME_TRANSFORMS = {"year", "month", "day", "hour"}
TIMESTAMP_TYPES = {"timestamp", "timestamptz", "timestamp_ns", "timestamptz_ns"}
JAVA_LETTER_CATEGORIES = {"Lu", "Ll", "Lt", "Lm", "Lo"}
JAVA_DIGIT_CATEGORY = "Nd"


@dataclass
class PartitionSpecField:
    name: str
    field_id: int
    transform: str
    source_type: Optional[str] = None


def build_partition_spec_fields(partition_specs: Optional[List[dict]], schemas: Optional[List[dict]] = None) -> Dict[int, List[PartitionSpecField]]:
    source_types = {}
    pending_types = sorted(schemas or [], key=lambda schema: schema.get("schema-id", 0), reverse=True)
    while pending_types:
        for source_field in pending_types.pop().get("fields", []):
            source_type = source_field["type"]
            if isinstance(source_type, str):
                source_types[int(source_field["id"])] = source_type
            elif isinstance(source_type, dict):
                pending_types.append(source_type)

    spec_fields_by_id = {}
    for spec in partition_specs or []:
        spec_fields = []
        for index, spec_field in enumerate(spec.get("fields") or []):
            field_id = spec_field.get("field-id")
            if field_id is None:
                field_id = V1_PARTITION_FIELD_ID_START + index
            source_ids = spec_field.get("source-ids") or [spec_field.get("source-id")]
            source_type = source_types.get(source_ids[0]) if len(source_ids) == 1 else None
            spec_fields.append(PartitionSpecField(spec_field["name"], int(field_id), str(spec_field.get("transform", "")), source_type))

        spec_fields_by_id[int(spec.get("spec-id", 0))] = spec_fields

    return spec_fields_by_id


def resolve_stored_partition_field_names(
    spec_fields_by_id: Dict[int, List[PartitionSpecField]], stored_field_names: List[str]
) -> Optional[Dict[int, str]]:
    names_by_field_id = {}
    resolved_names = None
    for spec_id in sorted(spec_fields_by_id):
        for spec_field in spec_fields_by_id[spec_id]:
            names_by_field_id[spec_field.field_id] = spec_field.name

        for escaped in (False, True):
            candidate_names = {field_id: _avro_compatible_name(name) if escaped else name for field_id, name in names_by_field_id.items()}
            first_match = _match_stored_partition_field_names(candidate_names, stored_field_names, reverse=False)
            if first_match is None:
                continue

            last_match = _match_stored_partition_field_names(candidate_names, stored_field_names, reverse=True)
            if first_match != last_match or (resolved_names is not None and resolved_names != first_match):
                return None

            resolved_names = first_match

    return resolved_names


def _match_stored_partition_field_names(names_by_field_id: Dict[int, str], stored_field_names: List[str], reverse: bool) -> Optional[Dict[int, str]]:
    stored_names = stored_field_names[::-1] if reverse else stored_field_names
    field_index = 0
    matched_names = {}
    for field_id in sorted(names_by_field_id, reverse=reverse):
        if field_index == len(stored_names):
            break

        if names_by_field_id[field_id] == stored_names[field_index]:
            matched_names[field_id] = stored_names[field_index]
            field_index += 1

    return matched_names if field_index == len(stored_names) else None


def build_partition(
    raw_partition: Optional[dict],
    spec_fields: Optional[List[PartitionSpecField]],
    stored_names_by_field_id: Optional[Dict[int, str]] = None,
) -> List[Dict[str, Any]]:
    raw_partition = raw_partition or {}
    if spec_fields is None:
        return [_partition_entry(name, None, None, value) for name, value in raw_partition.items()]

    entries = []
    for spec_field in spec_fields:
        stored_name = spec_field.name if stored_names_by_field_id is None else stored_names_by_field_id.get(spec_field.field_id)
        value = _raw_partition_value(raw_partition, stored_name) if stored_name is not None else None
        entries.append(_partition_entry(spec_field.name, spec_field.field_id, spec_field.transform, value, spec_field.source_type))

    return entries


def _raw_partition_value(raw_partition: dict, name: str) -> Any:
    if name in raw_partition:
        return raw_partition[name]

    return raw_partition.get(_avro_compatible_name(name))


def _avro_compatible_name(name: str) -> str:
    utf16_bytes = name.encode("utf-16-be", "surrogatepass")
    sanitized = []
    for index in range(0, len(utf16_bytes), 2):
        code_unit = int.from_bytes(utf16_bytes[index : index + 2], "big")
        character = chr(code_unit)
        category = unicodedata.category(character)
        is_letter = category in JAVA_LETTER_CATEGORIES
        is_digit = category == JAVA_DIGIT_CATEGORY
        if character == "_" or is_letter or (index > 0 and is_digit):
            sanitized.append(character)
        elif is_digit:
            sanitized.append(f"_{character}")
        else:
            sanitized.append(f"_x{code_unit:X}")

    return "".join(sanitized)


def unique_partitions(partitions: List[List[Dict[str, Any]]]) -> List[List[Dict[str, Any]]]:
    partitions_by_key = {}
    for partition in partitions:
        key = tuple((entry["name"], entry["field_id"], repr(entry["value"])) for entry in partition)
        partitions_by_key.setdefault(key, partition)

    return sorted(partitions_by_key.values(), key=_partition_label)


def _partition_label(partition: List[Dict[str, Any]]) -> str:
    return ", ".join(f"{entry['name']} = {entry['formatted_value']}" for entry in partition)


def _partition_entry(name: str, field_id: Optional[int], transform: Optional[str], value: Any, source_type: Optional[str] = None) -> Dict[str, Any]:
    return {
        "name": name,
        "field_id": field_id,
        "value": _json_safe_value(value),
        "formatted_value": _format_value(transform, value, source_type),
    }


def _json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value

    if isinstance(value, float) and math.isfinite(value):
        return value

    return _format_plain_value(value)


def _format_value(transform: Optional[str], value: Any, source_type: Optional[str]) -> str:
    if value is None:
        return "null"

    if transform in TIME_TRANSFORMS and isinstance(value, int) and not isinstance(value, bool):
        with suppress(Exception):
            text = _format_time_transform_value(transform, value)

            return text + _timezone_label(source_type) if transform == "hour" else text

    if transform == "identity" and source_type in TIMESTAMP_TYPES:
        if isinstance(value, int) and not isinstance(value, bool):
            text = decode_nanosecond_timestamp(value) if source_type.endswith("_ns") else decode_timestamp(value)
            if text == str(value):
                return text

            text = text.removesuffix("+00:00").replace("T", " ")
        else:
            text = _format_plain_value(value)

        return text + _timezone_label(source_type)

    return _format_plain_value(value)


def _timezone_label(source_type: Optional[str]) -> str:
    if source_type in {"timestamptz", "timestamptz_ns"}:
        return " UTC"

    if source_type in {"timestamp", "timestamp_ns"}:
        return " (no timezone)"

    return " (timezone unknown)"


def _format_time_transform_value(transform: str, value: int) -> str:
    if transform == "year":
        return f"{1970 + value:04d}"

    if transform == "month":
        return f"{1970 + value // 12:04d}-{value % 12 + 1:02d}"

    if transform == "day":
        return arrow.get(0).shift(days=value).format("YYYY-MM-DD")

    return arrow.get(0).shift(hours=value).format("YYYY-MM-DD HH:00")


def _format_plain_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()

    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()

    if isinstance(value, datetime):
        return value.isoformat(sep=" ")

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return format(value, "f")

    return str(value)
