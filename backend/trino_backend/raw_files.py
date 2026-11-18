import hashlib
import threading
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set

from env import Env
from icegraph_logger import logger
from trino_backend.trino_client import Row, run_query, sql_string, sql_strings

MISSING_FILE_ERROR = "File not found, empty, or unreadable through the Trino Hive catalog: {file_path}"
PATHS_PER_PREDICATE = 500
PATHS_PER_QUERY = 1000


@dataclass(frozen=True)
class RawFileTable:
    kind: str
    file_format: str
    columns: str
    avro_schema: Optional[str] = None

    def table_properties(self, directory: str) -> str:
        properties = f"format = {sql_string(self.file_format)}, external_location = {sql_string(directory)}"
        if self.avro_schema is None:
            return properties

        return f"{properties}, extra_properties = MAP(ARRAY['avro.schema.literal'], ARRAY[{sql_string(self.avro_schema)}])"


TEXT_FILES = RawFileTable("text", "TEXTFILE", "line varchar")

_registered_tables: Set[str] = set()
_registered_tables_lock = threading.Lock()


def raw_files_union_sql(raw_file_table: RawFileTable, file_paths: Iterable[str], select: str) -> str:
    branches = []
    for directory, directory_file_paths in _group_by_directory(file_paths).items():
        table_name = _register_table(raw_file_table, directory)
        for batch in path_batches(directory_file_paths, PATHS_PER_PREDICATE):
            branches.append(f'SELECT "$path" AS file_path, {select} FROM {table_name} WHERE "$path" IN ({sql_strings(batch)})')

    if not branches:
        raise ValueError(f"No {raw_file_table.kind} files to read")

    return "\nUNION ALL\n".join(branches)


def read_raw_files(raw_file_table: RawFileTable, file_paths: Iterable[str], select: str) -> List[Row]:
    rows = []
    for batch in path_batches(file_paths, PATHS_PER_QUERY):
        rows.extend(run_query(raw_files_union_sql(raw_file_table, batch, select)))

    return rows


def find_existing_files(file_paths: Iterable[str]) -> Set[str]:
    existing_files = set()
    for batch in path_batches(file_paths, PATHS_PER_QUERY):
        rows = run_query(f"SELECT DISTINCT file_path FROM ({raw_files_union_sql(TEXT_FILES, batch, '1 AS present')})")
        existing_files.update(row.file_path for row in rows)

    return existing_files


def path_batches(file_paths: Iterable[str], batch_size: int) -> List[List[str]]:
    ordered_paths = sorted(set(file_paths))

    return [ordered_paths[start : start + batch_size] for start in range(0, len(ordered_paths), batch_size)]


def _group_by_directory(file_paths: Iterable[str]) -> Dict[str, Set[str]]:
    directories: Dict[str, Set[str]] = {}
    for file_path in file_paths:
        directory = file_path.rsplit("/", 1)[0]
        directories.setdefault(directory, set()).add(file_path)

    return directories


def _register_table(raw_file_table: RawFileTable, directory: str) -> str:
    table_properties = raw_file_table.table_properties(directory)
    table_key = f"{raw_file_table.columns}|{table_properties}"
    table_name = f"{Env.TRINO_RAW_FILES_SCHEMA}.icegraph_{raw_file_table.kind}_{hashlib.sha256(table_key.encode()).hexdigest()[:24]}"

    with _registered_tables_lock:
        if table_name in _registered_tables:
            return table_name

        logger.info(f"Registering raw {raw_file_table.kind} files table {table_name} for {directory}")
        run_query(f"CREATE TABLE IF NOT EXISTS {table_name} ({raw_file_table.columns}) WITH ({table_properties})")
        _registered_tables.add(table_name)

    return table_name
