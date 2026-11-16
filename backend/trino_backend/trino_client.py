from dataclasses import dataclass
from typing import Any, List

import arrow
import trino
from trino.auth import BasicAuthentication
from trino.types import NamedRowTuple

from constants import STANDART_DATE_FORMAT
from env import Env


class TrinoAnalysisError(Exception):
    pass


class Row(dict):
    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]

        except KeyError as error:
            raise AttributeError(name) from error

    def asDict(self, recursive: bool = False) -> dict:
        return dict(self)


@dataclass(frozen=True)
class TableName:
    catalog: str
    schema: str
    table: str


def run_query(sql: str) -> List[Row]:
    connection = trino.dbapi.connect(
        host=Env.TRINO_HOST,
        port=Env.TRINO_PORT,
        user=Env.TRINO_USER,
        http_scheme=Env.TRINO_HTTP_SCHEME,
        auth=BasicAuthentication(Env.TRINO_USER, Env.TRINO_PASSWORD) if Env.TRINO_PASSWORD else None,
        source="icegraph",
    )
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        column_names = [column[0] for column in cursor.description or []]

    finally:
        connection.close()

    return [Row({name: _to_python(value) for name, value in zip(column_names, row)}) for row in rows]


def _to_python(value: Any) -> Any:
    if isinstance(value, NamedRowTuple):
        return {name: _to_python(item) for name, item in zip(value._names, value)}

    if isinstance(value, list):
        return [_to_python(item) for item in value]

    if isinstance(value, dict):
        return {key: _to_python(item) for key, item in value.items()}

    return value


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_strings(values) -> str:
    return ", ".join(sql_string(value) for value in values)


def parse_table_name(table_name: str) -> TableName:
    parts = (table_name or "").split(".")
    if len(parts) != 3 or not all(parts):
        raise TrinoAnalysisError(f"Table '{table_name}' must be written as catalog.schema.table for the Trino backend.")

    return TableName(*parts)


def qualified_table(table_name: str) -> str:
    name = parse_table_name(table_name)

    return f"{sql_identifier(name.catalog)}.{sql_identifier(name.schema)}.{sql_identifier(name.table)}"


def metadata_table(table_name: str, metadata_table_name: str) -> str:
    name = parse_table_name(table_name)

    return f"{sql_identifier(name.catalog)}.{sql_identifier(name.schema)}.{sql_identifier(f'{name.table}${metadata_table_name}')}"


def utc_text(expression: str) -> str:
    return f"format_datetime({expression} AT TIME ZONE 'UTC', '{STANDART_DATE_FORMAT}')"


def timestamp_bound(column: str, operator: str, bound: arrow.Arrow) -> str:
    if bound in (arrow.Arrow.min, arrow.Arrow.max):
        return "TRUE"

    return f"{column} {operator} from_iso8601_timestamp({sql_string(bound.to('UTC').isoformat())})"
