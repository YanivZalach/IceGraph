from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

from trino_backend.raw_files import MISSING_FILE_ERROR, TEXT_FILES, raw_files_union_sql
from trino_backend.trino_client import Row, run_query, sql_string, sql_strings

COMPRESSED_METADATA_SUFFIXES = (".gz.metadata.json", ".metadata.json.gz")
COMPRESSED_METADATA_ERROR = "Metadata file {file_path} is compressed, which the Trino backend can't read."
MULTI_LINE_METADATA_ERROR = (
    "Metadata file {file_path} spans several lines. The Trino backend only reads metadata JSON written on one line, as Iceberg writes it by default."
)
INVALID_METADATA_ERROR = "Metadata file {file_path} is not a JSON object."


@dataclass
class MetadataJsonRead:
    rows: Dict[str, Row] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


def read_metadata_json(file_paths: Iterable[str], select: List[str]) -> MetadataJsonRead:
    result = MetadataJsonRead()
    readable_file_paths = set()
    for file_path in file_paths:
        if file_path.endswith(COMPRESSED_METADATA_SUFFIXES):
            result.errors[file_path] = COMPRESSED_METADATA_ERROR.format(file_path=file_path)
        else:
            readable_file_paths.add(file_path)

    if not readable_file_paths:
        return result

    lines_sql = raw_files_union_sql(TEXT_FILES, readable_file_paths, 'line, count(*) OVER (PARTITION BY "$path") AS line_count')
    rows = run_query(f"""
        SELECT file_path, line_count, m IS NOT NULL AS parsed, {", ".join(select)}
        FROM (
            SELECT file_path, line_count, IF(line_count = 1, TRY(CAST(json_parse(line) AS map(varchar, json)))) AS m
            FROM ({lines_sql})
        )
    """)

    for row in rows:
        if row.line_count > 1:
            result.errors[row.file_path] = MULTI_LINE_METADATA_ERROR.format(file_path=row.file_path)
        elif not row.parsed:
            result.errors[row.file_path] = INVALID_METADATA_ERROR.format(file_path=row.file_path)
        else:
            result.rows[row.file_path] = row

    for file_path in readable_file_paths:
        if file_path not in result.rows and file_path not in result.errors:
            result.errors[file_path] = MISSING_FILE_ERROR.format(file_path=file_path)

    return result


def json_value(key: str) -> str:
    return f"element_at(m, {sql_string(key)})"


def json_text(key: str) -> str:
    return f"json_format({json_value(key)})"


def json_object_with_keys(keys: Iterable[str], exclude: bool = False) -> str:
    condition = f"contains(ARRAY[{sql_strings(keys)}], k)"
    if exclude:
        condition = f"NOT {condition}"

    return f"json_format(CAST(map_filter(m, (k, v) -> {condition}) AS json))"


def snapshot_fields(fields: Iterable[str]) -> str:
    values = ", ".join(f"json_extract_scalar(snapshot, {sql_string(json_path(field_name))})" for field_name in fields)

    return f"transform(CAST({json_value('snapshots')} AS array(json)), snapshot -> ARRAY[{values}])"


def json_path(key: str) -> str:
    return f'$["{key}"]'


def normalize_entries(entries: Optional[List[Dict[str, Any]]], field_names: List[str]) -> Optional[List[Dict[str, Any]]]:
    if entries is None:
        return None

    return [{field_name: entry.get(field_name) for field_name in field_names} for entry in entries]
