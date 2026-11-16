import base64
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from collectors.utils import normalize_legacy_v1_metadata_dict
from trino_backend.metadata_json import json_text, read_metadata_json
from trino_backend.table_utils import find_current_metadata_file
from trino_backend.trino_client import TrinoAnalysisError, sql_identifier

AVRO = "avro"
PARQUET = "parquet"

PLAIN_TYPES = {
    "boolean": ("boolean", "boolean"),
    "int": ("integer", "int"),
    "long": ("bigint", "long"),
    "float": ("real", "float"),
    "double": ("double", "double"),
    "date": ("date", {"type": "int", "logicalType": "date"}),
    "string": ("varchar", "string"),
    "binary": ("varbinary", "bytes"),
}
MICROSECOND_TYPES = {
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
    "timestamp_ns": "timestamp_ns",
    "timestamptz_ns": "timestamp_ns",
    "time": "time",
}
DECIMAL_TYPE_PATTERN = re.compile(r"decimal\((\d+),\s*(\d+)\)")
FIXED_TYPE_PATTERN = re.compile(r"fixed\[(\d+)\]")
UUID_SIZE = 16
EPOCH = datetime(1970, 1, 1)
UNREADABLE_PARTITION_FIELDS_WARNING = "The Trino backend can't read partition fields of type {types}, so these fields are not shown: {fields}."


@dataclass
class TableLayout:
    schemas: List[Dict[str, Any]]
    partition_specs: Dict[int, List[Dict[str, Any]]]


@dataclass
class PartitionColumn:
    name: str
    trino_type: str
    avro_type: Any
    value_kind: str = "plain"
    scale: int = 0
    needs_avro_schema: bool = False


@dataclass
class PartitionRowType:
    columns: List[PartitionColumn] = field(default_factory=list)
    unreadable_fields: Dict[str, str] = field(default_factory=dict)

    @property
    def needs_avro_schema(self) -> bool:
        for column in self.columns:
            if column.needs_avro_schema:
                return True

        return False

    def ddl(self) -> Optional[str]:
        if not self.columns:
            return None

        return "row(" + ", ".join(f"{sql_identifier(column.name)} {column.trino_type}" for column in self.columns) + ")"

    def avro_fields(self) -> List[Dict[str, Any]]:
        defined_type_names = set()
        fields = []
        for column in self.columns:
            avro_type = column.avro_type
            if isinstance(avro_type, dict) and "name" in avro_type:
                if avro_type["name"] in defined_type_names:
                    avro_type = avro_type["name"]
                else:
                    defined_type_names.add(avro_type["name"])
            fields.append({"name": column.name, "type": ["null", avro_type], "default": None})

        return fields

    def json_sql(self, partition_column: str) -> str:
        fields = []
        field_types = []
        for column in self.columns:
            field = f"{partition_column}.{sql_identifier(column.name.lower())}"
            if column.trino_type == "varbinary":
                fields.append(f"to_base64({field})")
                field_types.append(f"{sql_identifier(column.name)} varchar")
            else:
                fields.append(field)
                field_types.append(f"{sql_identifier(column.name)} {column.trino_type}")

        return f"json_format(CAST(CAST(ROW({', '.join(fields)}) AS row({', '.join(field_types)})) AS json))"

    def to_python(self, partition_json: Optional[str]) -> Dict[str, Any]:
        values = json.loads(partition_json, parse_float=Decimal) if partition_json else {}
        values = {key.lower(): value for key, value in values.items()}

        return {column.name: _convert_value(column, values.get(column.name.lower())) for column in self.columns}

    def warning(self) -> Optional[str]:
        if not self.unreadable_fields:
            return None

        types = ", ".join(sorted(set(self.unreadable_fields.values())))

        return UNREADABLE_PARTITION_FIELDS_WARNING.format(types=types, fields=", ".join(sorted(self.unreadable_fields)))


def load_table_layout(table_name: str) -> TableLayout:
    metadata_path = find_current_metadata_file(table_name)
    keys = ["schemas", "schema", "partition-specs", "partition-spec"]
    metadata_read = read_metadata_json([metadata_path], [f"{json_text(key)} AS {sql_identifier(key)}" for key in keys])
    if metadata_path in metadata_read.errors:
        raise TrinoAnalysisError(metadata_read.errors[metadata_path])

    row = metadata_read.rows[metadata_path]
    metadata = {key: json.loads(row[key]) for key in keys if row[key] is not None}
    normalize_legacy_v1_metadata_dict(metadata)

    return TableLayout(
        schemas=metadata.get("schemas") or [],
        partition_specs={spec["spec-id"]: spec["fields"] for spec in metadata.get("partition-specs") or []},
    )


def partition_row_type(layout: TableLayout, spec_ids: Iterable[int], file_format: str) -> PartitionRowType:
    source_type_versions = _source_type_versions_by_field_id(layout.schemas)
    columns: Dict[str, PartitionColumn] = {}
    unreadable_fields: Dict[str, str] = {}

    for spec_id in spec_ids:
        for partition_field in layout.partition_specs.get(spec_id, []):
            name = make_compatible_name(partition_field["name"])
            source_types = source_type_versions.get(partition_field["source-id"]) or [None]
            result_type = _transform_result_type(partition_field["transform"], source_types[-1])
            column = _partition_column(name, result_type, file_format)
            if column is not None and column.needs_avro_schema and len(set(map(str, source_types))) > 1:
                column = None
            if column is None:
                unreadable_fields[name] = _type_label(result_type)
                columns.pop(name, None)
            else:
                columns[name] = column
                unreadable_fields.pop(name, None)

    return PartitionRowType(columns=list(columns.values()), unreadable_fields=unreadable_fields)


def make_compatible_name(name: str) -> str:
    if _is_valid_avro_name(name):
        return name

    sanitized = [name[0] if name[0].isalpha() or name[0] == "_" else _sanitize_character(name[0])]
    for character in name[1:]:
        sanitized.append(character if character.isalnum() or character == "_" else _sanitize_character(character))

    return "".join(sanitized)


def _is_valid_avro_name(name: str) -> bool:
    if not name or not (name[0].isalpha() or name[0] == "_"):
        return False

    for character in name[1:]:
        if not (character.isalnum() or character == "_"):
            return False

    return True


def _sanitize_character(character: str) -> str:
    if character.isdigit():
        return f"_{character}"

    return f"_x{format(ord(character), 'X')}"


def _source_type_versions_by_field_id(schemas: List[Dict[str, Any]]) -> Dict[int, List[Any]]:
    source_types: Dict[int, List[Any]] = {}
    for schema in sorted(schemas, key=lambda candidate: candidate.get("schema-id", 0)):
        _collect_field_types(schema, source_types)

    return source_types


def _collect_field_types(iceberg_type: Any, source_types: Dict[int, List[Any]]) -> None:
    if not isinstance(iceberg_type, dict):
        return

    if iceberg_type.get("type") == "struct":
        for struct_field in iceberg_type.get("fields", []):
            _add_field_type(source_types, struct_field["id"], struct_field["type"])
            _collect_field_types(struct_field["type"], source_types)

    elif iceberg_type.get("type") == "list":
        _add_field_type(source_types, iceberg_type["element-id"], iceberg_type["element"])
        _collect_field_types(iceberg_type["element"], source_types)

    elif iceberg_type.get("type") == "map":
        _add_field_type(source_types, iceberg_type["key-id"], iceberg_type["key"])
        _add_field_type(source_types, iceberg_type["value-id"], iceberg_type["value"])
        _collect_field_types(iceberg_type["key"], source_types)
        _collect_field_types(iceberg_type["value"], source_types)


def _add_field_type(source_types: Dict[int, List[Any]], field_id: int, field_type: Any) -> None:
    field_types = source_types.setdefault(field_id, [])
    if field_type not in field_types:
        field_types.append(field_type)


def _transform_result_type(transform: str, source_type: Any) -> Any:
    if transform.startswith("bucket") or transform in ("year", "month", "hour"):
        return "int"

    if transform == "day":
        return "date"

    return source_type


def _partition_column(name: str, iceberg_type: Any, file_format: str) -> Optional[PartitionColumn]:
    if not isinstance(iceberg_type, str):
        return None

    if iceberg_type in PLAIN_TYPES:
        trino_type, avro_type = PLAIN_TYPES[iceberg_type]

        return PartitionColumn(name, trino_type, avro_type)

    if iceberg_type in MICROSECOND_TYPES:
        return PartitionColumn(name, "bigint", "long", MICROSECOND_TYPES[iceberg_type])

    if iceberg_type == "uuid":
        return PartitionColumn(name, "varbinary", _fixed("uuid_fixed", UUID_SIZE), "uuid", needs_avro_schema=file_format == AVRO)

    fixed_match = FIXED_TYPE_PATTERN.fullmatch(iceberg_type)
    if fixed_match:
        size = int(fixed_match.group(1))

        return PartitionColumn(name, "varbinary", _fixed(f"fixed_{size}", size), needs_avro_schema=file_format == AVRO)

    decimal_match = DECIMAL_TYPE_PATTERN.fullmatch(iceberg_type)
    if decimal_match:
        precision, scale = int(decimal_match.group(1)), int(decimal_match.group(2))
        if file_format == PARQUET:
            return PartitionColumn(name, f"decimal({precision},{scale})", None)

        avro_type = _fixed(f"decimal_{precision}_{scale}", _decimal_required_bytes(precision))

        return PartitionColumn(name, "varbinary", avro_type, "decimal", scale=scale, needs_avro_schema=True)

    return None


def _fixed(name: str, size: int) -> Dict[str, Any]:
    return {"type": "fixed", "name": name, "size": size}


def _decimal_required_bytes(precision: int) -> int:
    required_bytes = 1
    while 2 ** (8 * required_bytes - 1) < 10**precision:
        required_bytes += 1

    return required_bytes


def _type_label(iceberg_type: Any) -> str:
    if not isinstance(iceberg_type, str):
        return "unknown"

    return iceberg_type.split("(")[0].split("[")[0]


def _convert_value(column: PartitionColumn, value: Any) -> Any:
    if value is None:
        return None

    if column.trino_type == "varbinary":
        value = bytearray(base64.b64decode(value))

    if column.value_kind == "plain":
        return value

    if column.value_kind == "uuid":
        return str(uuid.UUID(bytes=bytes(value)))

    if column.value_kind == "decimal":
        return Decimal(int.from_bytes(value, "big", signed=True)).scaleb(-column.scale)

    if column.value_kind == "timestamp":
        return str(EPOCH + timedelta(microseconds=value))

    if column.value_kind == "timestamp_ns":
        return str(EPOCH + timedelta(microseconds=value // 1000))

    return str((datetime.combine(EPOCH, time()) + timedelta(microseconds=value)).time())
