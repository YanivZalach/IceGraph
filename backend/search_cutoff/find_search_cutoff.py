from base_classes.utils import column_to_string_utc
from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional

import arrow
import polars as pl
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from constants import SNAPSHOT_METADATA_WINDOW_WARNING
from icegraph_logger import logger
from extractors.metadata_log_entries_extractor import MetadataLogEntriesExtractor
from base_classes.utils import epoch_millis_to_arrow_utc, timed, to_arrow_utc


@dataclass
class SearchCutoff:
    start_snapshot_cutoff: arrow.Arrow = arrow.Arrow.min
    end_snapshot_cutoff: arrow.Arrow = arrow.Arrow.max
    start_metadata_cutoff: arrow.Arrow = arrow.Arrow.min
    end_metadata_cutoff: arrow.Arrow = arrow.Arrow.max
    manifests_to_ignore_df: DataFrame = field(default_factory=None)
    warnings: Dict[str, List[str]] = field(default_factory=dict)

    @classmethod
    def with_spark(cls, spark: SparkSession) -> "SearchCutoff":
        return cls(manifests_to_ignore_df=_create_empty_manifests_to_ignore_df(spark))


class StartCutoffs(NamedTuple):
    snapshot_cutoff: arrow.Arrow
    metadata_cutoff: arrow.Arrow
    manifests_to_ignore_df: DataFrame


class EndCutoffs(NamedTuple):
    snapshot_cutoff: arrow.Arrow
    metadata_cutoff: arrow.Arrow


@timed
def find_search_cutoff(
    spark: SparkSession,
    table_name: str,
    start_snapshot_id: Optional[int],
    end_snapshot_id: Optional[int],
) -> SearchCutoff:
    cutoff = SearchCutoff.with_spark(spark)
    metadata_log_entries_extractor = MetadataLogEntriesExtractor(table_name)

    if start_snapshot_id:
        start = _get_start_cutoffs(spark, table_name, start_snapshot_id, metadata_log_entries_extractor, cutoff.warnings)
        cutoff.start_snapshot_cutoff = start.snapshot_cutoff
        cutoff.start_metadata_cutoff = start.metadata_cutoff
        cutoff.manifests_to_ignore_df = start.manifests_to_ignore_df

    if end_snapshot_id:
        end = _get_end_cutoffs(spark, table_name, end_snapshot_id, metadata_log_entries_extractor, cutoff.warnings)
        cutoff.end_snapshot_cutoff = end.snapshot_cutoff
        cutoff.end_metadata_cutoff = end.metadata_cutoff

    if cutoff.start_snapshot_cutoff > cutoff.end_snapshot_cutoff:
        raise ValueError("Start snapshot is after end snapshot")

    return cutoff


def _get_start_cutoffs(
    spark: SparkSession,
    table_name: str,
    start_snapshot_id: int,
    metadata_log_entries_extractor: MetadataLogEntriesExtractor,
    warnings: Dict[str, List[str]],
) -> StartCutoffs:
    row = (
        spark.sql(f"""
        SELECT committed_at, parent_id
        FROM {table_name}.snapshots
        WHERE snapshot_id = {start_snapshot_id}
    """)
        .withColumn("committed_at", column_to_string_utc("committed_at"))
        .first()
    )

    if not row:
        raise ValueError(f"Start snapshot {start_snapshot_id} does not exist in table {table_name}")

    snapshot_cutoff = to_arrow_utc(row.committed_at)

    metadata_timestamp_ms = metadata_log_entries_extractor.extract_entries().filter(pl.col("snapshot_id") == start_snapshot_id)["timestamp_ms"].min()

    if metadata_timestamp_ms is not None:
        metadata_cutoff = epoch_millis_to_arrow_utc(metadata_timestamp_ms)
    else:
        _warn_commit_time_boundary(warnings, "start", start_snapshot_id, metadata_log_entries_extractor.missing_snapshot_reason)
        metadata_cutoff = snapshot_cutoff

    manifests_to_ignore_df = _get_manifests_to_ignore_df(spark, table_name, row.parent_id)

    return StartCutoffs(
        snapshot_cutoff=snapshot_cutoff,
        metadata_cutoff=metadata_cutoff,
        manifests_to_ignore_df=manifests_to_ignore_df,
    )


def _get_end_cutoffs(
    spark: SparkSession,
    table_name: str,
    end_snapshot_id: int,
    metadata_log_entries_extractor: MetadataLogEntriesExtractor,
    warnings: Dict[str, List[str]],
) -> EndCutoffs:
    row = (
        spark.sql(f"""
        SELECT committed_at
        FROM {table_name}.snapshots
        WHERE snapshot_id = {end_snapshot_id}
    """)
        .withColumn("committed_at", column_to_string_utc("committed_at"))
        .first()
    )

    if not row:
        raise ValueError(f"End snapshot {end_snapshot_id} does not exist in table {table_name}")

    snapshot_cutoff = to_arrow_utc(row.committed_at)

    metadata_timestamp_ms = metadata_log_entries_extractor.extract_entries().filter(pl.col("snapshot_id") == end_snapshot_id)["timestamp_ms"].max()

    if metadata_timestamp_ms is not None:
        metadata_cutoff = epoch_millis_to_arrow_utc(metadata_timestamp_ms)
    else:
        _warn_commit_time_boundary(warnings, "end", end_snapshot_id, metadata_log_entries_extractor.missing_snapshot_reason)
        metadata_cutoff = snapshot_cutoff

    return EndCutoffs(
        snapshot_cutoff=snapshot_cutoff,
        metadata_cutoff=metadata_cutoff,
    )


def _warn_commit_time_boundary(warnings: Dict[str, List[str]], label: str, snapshot_id: int, reason: str) -> None:
    warnings[f"{label}_snapshot_metadata_window"] = [SNAPSHOT_METADATA_WINDOW_WARNING.format(label=label, snapshot_id=snapshot_id, reason=reason)]


def _get_manifests_to_ignore_df(
    spark: SparkSession,
    table_name: str,
    parent_id: Optional[int],
) -> DataFrame:
    if parent_id is None:
        return _create_empty_manifests_to_ignore_df(spark)

    try:
        parent_manifest_list = spark.sql(f"""
            SELECT manifest_list
            FROM {table_name}.snapshots
            WHERE snapshot_id = {parent_id}
        """).first()["manifest_list"]

        return spark.read.format("avro").load(parent_manifest_list).select(F.col("manifest_path").alias("path"))

    except:
        logger.warning(
            f"[{table_name}] Failed to load parent manifest list for snapshot {parent_id} . Likely already cleaned up, which is expected",
            exc_info=True,
        )
        return _create_empty_manifests_to_ignore_df(spark)


def _create_empty_manifests_to_ignore_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], StructType([StructField("path", StringType())]))
