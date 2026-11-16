import arrow

from env import Env
from icegraph_logger import logger
from table_list_catalog.table_list_catalog import CacheEntry, TableListCatalog
from trino_backend.table_utils import ICEBERG_CONNECTOR_NAME
from trino_backend.trino_client import Row, run_query, sql_identifier, sql_strings

SYSTEM_CATALOG = "system"
INFORMATION_SCHEMA = "information_schema"
ICEBERG_SYSTEM_SCHEMA = "system"


class TrinoTableListCatalog(TableListCatalog):
    def __init__(self):
        pass

    def _collect_candidates(self) -> CacheEntry:
        run_time = arrow.utcnow()

        tables = []
        for catalog in self._included_catalogs():
            try:
                tables.extend(self._catalog_tables(catalog.catalog_name, self._excluded_schemas(catalog)))

            except Exception as error:
                logger.warning(f"Could not list tables in catalog {catalog.catalog_name}: {error}")

        return CacheEntry(run_time, sorted(tables))

    @staticmethod
    def _included_catalogs() -> list[Row]:
        rows = run_query("SELECT catalog_name, connector_name FROM system.metadata.catalogs")

        return [
            row
            for row in rows
            if row.catalog_name != SYSTEM_CATALOG and (Env.INCLUDE_NONE_ICEBERG_CATALOGS or row.connector_name == ICEBERG_CONNECTOR_NAME)
        ]

    @staticmethod
    def _excluded_schemas(catalog: Row) -> list[str]:
        excluded_schemas = [INFORMATION_SCHEMA]
        if catalog.connector_name == ICEBERG_CONNECTOR_NAME:
            excluded_schemas.append(ICEBERG_SYSTEM_SCHEMA)

        raw_files_catalog, raw_files_schema = Env.TRINO_RAW_FILES_SCHEMA.split(".", 1)
        if raw_files_catalog == catalog.catalog_name:
            excluded_schemas.append(raw_files_schema)

        return excluded_schemas

    @staticmethod
    def _catalog_tables(catalog: str, excluded_schemas: list[str]) -> list[str]:
        rows = run_query(f"""
            SELECT table_schema, table_name
            FROM {sql_identifier(catalog)}.information_schema.tables
            WHERE table_schema NOT IN ({sql_strings(excluded_schemas)}) AND table_type = 'BASE TABLE'
        """)

        return [f"{catalog}.{row.table_schema}.{row.table_name}" for row in rows]
