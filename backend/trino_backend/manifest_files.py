import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from trino_backend.partition_layout import AVRO, PartitionRowType, TableLayout, partition_row_type
from trino_backend.raw_files import RawFileTable, raw_files_union_sql

MANIFEST_LIST_FILES = RawFileTable(
    "manifest_list", "AVRO", "manifest_path varchar, partition_spec_id integer, added_snapshot_id bigint, first_row_id bigint"
)

METRIC_VALUE_TYPES = {
    "column_sizes": ("bigint", "long"),
    "value_counts": ("bigint", "long"),
    "null_value_counts": ("bigint", "long"),
    "nan_value_counts": ("bigint", "long"),
    "lower_bounds": ("varbinary", "bytes"),
    "upper_bounds": ("varbinary", "bytes"),
}
DATA_FILE_FIELDS = {
    "content": ("integer", "int"),
    "file_path": ("varchar", "string"),
    "file_format": ("varchar", "string"),
    "record_count": ("bigint", "long"),
    "file_size_in_bytes": ("bigint", "long"),
    "key_metadata": ("varbinary", "bytes"),
    "split_offsets": ("array(bigint)", {"type": "array", "items": "long"}),
    "equality_ids": ("array(integer)", {"type": "array", "items": "int"}),
    "sort_order_id": ("integer", "int"),
    "first_row_id": ("bigint", "long"),
    "referenced_data_file": ("varchar", "string"),
    "content_offset": ("bigint", "long"),
    "content_size_in_bytes": ("bigint", "long"),
}


@dataclass(frozen=True)
class ManifestEntriesTable:
    raw_file_table: RawFileTable
    partition_type: PartitionRowType


def manifest_entries_table(layout: TableLayout, spec_ids: Iterable[int]) -> ManifestEntriesTable:
    partition_type = partition_row_type(layout, spec_ids, AVRO)
    data_file_columns = [f"{name} {trino_type}" for name, (trino_type, _) in DATA_FILE_FIELDS.items()]
    data_file_columns.extend(f"{name} array(row(key integer, value {trino_type}))" for name, (trino_type, _) in METRIC_VALUE_TYPES.items())
    partition_ddl = partition_type.ddl()
    if partition_ddl:
        data_file_columns.append(f"partition {partition_ddl}")

    avro_schema = _manifest_entry_avro_schema(partition_type) if partition_type.needs_avro_schema else None
    raw_file_table = RawFileTable("manifest", "AVRO", f"status integer, data_file row({', '.join(data_file_columns)})", avro_schema)

    return ManifestEntriesTable(raw_file_table, partition_type)


def manifest_entries_sql(entries_table: ManifestEntriesTable, manifest_paths: Iterable[str], select: List[str]) -> str:
    return raw_files_union_sql(entries_table.raw_file_table, manifest_paths, ", ".join(select))


def partition_json_sql(entries_table: ManifestEntriesTable) -> str:
    if not entries_table.partition_type.columns:
        return "CAST(NULL AS varchar)"

    return entries_table.partition_type.json_sql("data_file.partition")


def _manifest_entry_avro_schema(partition_type: PartitionRowType) -> str:
    data_file_fields = [_nullable_field(name, avro_type) for name, (_, avro_type) in DATA_FILE_FIELDS.items()]
    for name, (_, avro_type) in METRIC_VALUE_TYPES.items():
        entry_record = {"type": "record", "name": f"{name}_entry", "fields": [{"name": "key", "type": "int"}, {"name": "value", "type": avro_type}]}
        data_file_fields.append(_nullable_field(name, {"type": "array", "items": entry_record}))
    data_file_fields.append(_nullable_field("partition", {"type": "record", "name": "r102", "fields": partition_type.avro_fields()}))

    return json.dumps(
        {
            "type": "record",
            "name": "manifest_entry",
            "fields": [
                _nullable_field("status", "int"),
                _nullable_field("data_file", {"type": "record", "name": "r2", "fields": data_file_fields}),
            ],
        }
    )


def _nullable_field(name: str, avro_type: Any) -> Dict[str, Any]:
    return {"name": name, "type": ["null", avro_type], "default": None}
