import threading
from typing import Optional

import arrow
from pydantic import BaseModel, ConfigDict

from base_classes.utils import timed
from env import Env
from spark_connect import open_spark_connect_session
from table_list_catalog.utils import (
    collect_catalogs_tables_names,
    collect_databases_in_catalogs,
    filter_catalogs_to_include,
    get_spark_default_catalog,
    list_catalog_names,
)


class CacheEntry(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    timestamp: arrow.Arrow
    tables: list[str]


class TableListCatalog:
    _cache: Optional[CacheEntry] = None
    _cache_write_lock = threading.Lock()

    def __init__(self):
        self._spark = open_spark_connect_session()

    @timed
    def collect(self) -> list[str]:
        fresh_tables = self._fresh_cached_tables()

        if fresh_tables is not None:
            return fresh_tables

        with TableListCatalog._cache_write_lock:
            fresh_tables = self._fresh_cached_tables()
            if fresh_tables is not None:
                return fresh_tables

            cache_entry = self._collect_candidates()
            TableListCatalog._cache = cache_entry

        return cache_entry.tables

    @staticmethod
    def _fresh_cached_tables() -> Optional[list[str]]:
        cache = TableListCatalog._cache
        if cache and (arrow.utcnow() - cache.timestamp).total_seconds() < Env.TABLE_LIST_CACHE_TTL_SECONDS:
            return cache.tables
        return None

    def _collect_candidates(self) -> CacheEntry:
        run_time = arrow.utcnow()

        catalogs = list_catalog_names(self._spark)
        included_catalogs = filter_catalogs_to_include(self._spark, catalogs)
        databases = collect_databases_in_catalogs(self._spark, included_catalogs)

        tables = collect_catalogs_tables_names(self._spark, databases)
        spark_default_catalog = get_spark_default_catalog(self._spark)
        clean_tables = sorted([table.removeprefix(f"{spark_default_catalog}.") for table in tables])

        return CacheEntry(timestamp=run_time, tables=clean_tables)
