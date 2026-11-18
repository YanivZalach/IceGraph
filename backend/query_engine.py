from dataclasses import dataclass
from typing import Callable, Tuple, Type

from pyspark.errors import AnalysisException
from trino.exceptions import TrinoUserError

import base_classes.utils as spark_utils
import trino_backend.table_utils as trino_utils
from collectors.collect_table_description import TableDescriptionCollector
from collectors.collect_table_metadata import TableMetadataCollector
from env import Env
from snapshot_map.snapshot_mapping import collect_snapshot_map as collect_spark_snapshot_map
from spark_connect import close_spark_connect_session
from table_inventory.table_inventory import TableInventory
from table_list_catalog.table_list_catalog import TableListCatalog
from trino_backend.metadata import TrinoTableMetadataCollector
from trino_backend.snapshots import collect_snapshot_map as collect_trino_snapshot_map
from trino_backend.table_description import TrinoTableDescriptionCollector
from trino_backend.table_inventory import TrinoTableInventory
from trino_backend.table_list import TrinoTableListCatalog
from trino_backend.trino_client import TrinoAnalysisError

SPARK = "spark"
TRINO = "trino"


@dataclass(frozen=True)
class QueryEngine:
    name: str
    analysis_errors: Tuple[Type[Exception], ...]
    verify_iceberg_table: Callable
    collect_graph_metadata_file: Callable
    collect_snapshot_map: Callable
    table_inventory: Type
    table_description_collector: Type
    table_metadata_collector: Type
    table_list_catalog: Type
    close_session: Callable


def load_query_engine() -> QueryEngine:
    if Env.QUERY_ENGINE == SPARK:
        return QueryEngine(
            name="Spark",
            analysis_errors=(AnalysisException,),
            verify_iceberg_table=spark_utils.verify_iceberg_table,
            collect_graph_metadata_file=spark_utils.collect_graph_metadata_file,
            collect_snapshot_map=collect_spark_snapshot_map,
            table_inventory=TableInventory,
            table_description_collector=TableDescriptionCollector,
            table_metadata_collector=TableMetadataCollector,
            table_list_catalog=TableListCatalog,
            close_session=close_spark_connect_session,
        )

    if Env.QUERY_ENGINE == TRINO:
        return QueryEngine(
            name="Trino",
            analysis_errors=(TrinoAnalysisError, TrinoUserError),
            verify_iceberg_table=trino_utils.verify_iceberg_table,
            collect_graph_metadata_file=trino_utils.collect_graph_metadata_file,
            collect_snapshot_map=collect_trino_snapshot_map,
            table_inventory=TrinoTableInventory,
            table_description_collector=TrinoTableDescriptionCollector,
            table_metadata_collector=TrinoTableMetadataCollector,
            table_list_catalog=TrinoTableListCatalog,
            close_session=lambda: None,
        )

    raise ValueError(f"QUERY_ENGINE must be {SPARK} or {TRINO}, got {Env.QUERY_ENGINE!r}")
