from typing import Optional

import polars as pl

from base_classes.utils import timed
from extractors.utils import find_newest_file
from trino_backend.metadata_log_entries import TrinoMetadataLogEntriesExtractor
from trino_backend.trino_client import TrinoAnalysisError, metadata_table, parse_table_name, run_query, sql_identifier, sql_string

ICEBERG_CONNECTOR_NAME = "iceberg"


def verify_iceberg_table(table_name: str) -> bool:
    name = parse_table_name(table_name)
    rows = run_query(f"""
        SELECT
            (SELECT connector_name FROM system.metadata.catalogs WHERE catalog_name = {sql_string(name.catalog.lower())}) AS connector_name,
            (
                SELECT count(*)
                FROM {sql_identifier(name.catalog)}.information_schema.tables
                WHERE table_schema = {sql_string(name.schema.lower())} AND table_name = {sql_string(name.table.lower())} AND table_type = 'BASE TABLE'
            ) AS matching_tables
    """)

    if rows[0].matching_tables == 0:
        raise TrinoAnalysisError(f"Table '{table_name}' was not found.")

    if rows[0].connector_name != ICEBERG_CONNECTOR_NAME:
        raise TrinoAnalysisError(f"Table '{table_name}' is not an Iceberg table.")

    return True


@timed
def collect_graph_metadata_file(table_name: str, end_snapshot_id: Optional[int]) -> str:
    metadata_entries = TrinoMetadataLogEntriesExtractor(table_name).extract_entries()

    if end_snapshot_id is not None:
        metadata_entries = metadata_entries.filter(pl.col("snapshot_id") == end_snapshot_id)

    selected_file = find_newest_file(metadata_entries)

    if selected_file is None:
        if end_snapshot_id is None:
            raise ValueError(f"No metadata files found for table {table_name}")
        raise ValueError(f"No metadata file found for table {table_name} at snapshot {end_snapshot_id}")

    return selected_file


def find_current_metadata_file(table_name: str) -> str:
    rows = run_query(f"""
        SELECT file
        FROM {metadata_table(table_name, "metadata_log_entries")}
        ORDER BY timestamp DESC, length(file) DESC, file DESC
        LIMIT 1
    """)
    if not rows:
        raise ValueError(f"No metadata files found for table {table_name}")

    return rows[0].file
