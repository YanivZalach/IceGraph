from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional

import arrow
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from base_classes.utils import column_to_string_utc, timed, to_arrow_utc
from constants import METADATA_REFS_READ_WARNING, SNAPSHOT_METADATA_WINDOW_WARNING
from icegraph_logger import logger
from metadata_log.metadata_log import MetadataLog, SnapshotMetadataWindow
from spark_connect import open_spark_connect_session


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
    metadata_log: Optional[MetadataLog] = None,
) -> SearchCutoff:
    cutoff = SearchCutoff.with_spark(spark)
    selected_snapshot_ids = {snapshot_id for snapshot_id in (start_snapshot_id, end_snapshot_id) if snapshot_id}
    windows = _find_snapshot_windows(metadata_log or MetadataLog(spark, table_name), selected_snapshot_ids, table_name, cutoff.warnings)

    if start_snapshot_id:
        start = _get_start_cutoffs(spark, table_name, start_snapshot_id, windows.get(start_snapshot_id), cutoff.warnings)
        cutoff.start_snapshot_cutoff = start.snapshot_cutoff
        cutoff.start_metadata_cutoff = start.metadata_cutoff
        cutoff.manifests_to_ignore_df = start.manifests_to_ignore_df

    if end_snapshot_id:
        end = _get_end_cutoffs(spark, table_name, end_snapshot_id, windows.get(end_snapshot_id), cutoff.warnings)
        cutoff.end_snapshot_cutoff = end.snapshot_cutoff
        cutoff.end_metadata_cutoff = end.metadata_cutoff

    if cutoff.start_snapshot_cutoff > cutoff.end_snapshot_cutoff:
        raise ValueError("Start snapshot is after end snapshot")

    return cutoff


@timed
def collect_graph_metadata_file(table_name: str, end_snapshot_id: Optional[int]) -> str:
    spark = open_spark_connect_session()
    metadata_log = MetadataLog(spark, table_name)
    end_metadata_cutoff = arrow.Arrow.max
    if end_snapshot_id is not None:
        end_metadata_cutoff = find_search_cutoff(spark, table_name, None, end_snapshot_id, metadata_log).end_metadata_cutoff

    selected_entry = metadata_log.latest_entry_at_or_before(end_metadata_cutoff)
    if selected_entry is None:
        raise ValueError(f"No metadata files found for table {table_name}")

    return selected_entry.file


def _find_snapshot_windows(
    metadata_log: MetadataLog,
    snapshot_ids: set,
    table_name: str,
    warnings: Dict[str, List[str]],
) -> Dict[int, SnapshotMetadataWindow]:
    if not snapshot_ids:
        return {}

    try:
        return metadata_log.find_snapshot_windows(snapshot_ids)

    except Exception as e:
        logger.warning(f"[{table_name}] Failed to read the branches and tags from the current metadata file", exc_info=True)
        error_lines = str(e).strip().splitlines()
        warnings["metadata_refs_read"] = [METADATA_REFS_READ_WARNING.format(error=error_lines[0] if error_lines else type(e).__name__)]

        return {}


def _read_snapshot_row(spark: SparkSession, table_name: str, snapshot_id: int, label: str):
    row = (
        spark.sql(f"""
        SELECT committed_at, parent_id
        FROM {table_name}.snapshots
        WHERE snapshot_id = {snapshot_id}
    """)
        .withColumn("committed_at", column_to_string_utc("committed_at"))
        .first()
    )

    if not row:
        raise ValueError(f"{label} snapshot {snapshot_id} does not exist in table {table_name}")

    return row


def _warn_commit_time_boundary(warnings: Dict[str, List[str]], label: str, snapshot_id: int) -> None:
    if "metadata_refs_read" in warnings:
        return

    boundary = label.lower()
    warnings[f"{boundary}_snapshot_metadata_window"] = [SNAPSHOT_METADATA_WINDOW_WARNING.format(label=boundary, snapshot_id=snapshot_id)]


def _get_start_cutoffs(
    spark: SparkSession,
    table_name: str,
    start_snapshot_id: int,
    window: Optional[SnapshotMetadataWindow],
    warnings: Dict[str, List[str]],
) -> StartCutoffs:
    row = _read_snapshot_row(spark, table_name, start_snapshot_id, "Start")
    snapshot_cutoff = to_arrow_utc(row.committed_at)

    if window is None:
        _warn_commit_time_boundary(warnings, "Start", start_snapshot_id)
        metadata_cutoff = snapshot_cutoff
    else:
        metadata_cutoff = window.first_entry.timestamp

    return StartCutoffs(
        snapshot_cutoff=snapshot_cutoff,
        metadata_cutoff=metadata_cutoff,
        manifests_to_ignore_df=_get_manifests_to_ignore_df(spark, table_name, row.parent_id),
    )


def _get_end_cutoffs(
    spark: SparkSession,
    table_name: str,
    end_snapshot_id: int,
    window: Optional[SnapshotMetadataWindow],
    warnings: Dict[str, List[str]],
) -> EndCutoffs:
    row = _read_snapshot_row(spark, table_name, end_snapshot_id, "End")
    snapshot_cutoff = to_arrow_utc(row.committed_at)

    if window is None:
        _warn_commit_time_boundary(warnings, "End", end_snapshot_id)
        metadata_cutoff = snapshot_cutoff
    elif window.is_open_ended:
        metadata_cutoff = arrow.Arrow.max
    else:
        metadata_cutoff = window.last_entry.timestamp

    return EndCutoffs(snapshot_cutoff=snapshot_cutoff, metadata_cutoff=metadata_cutoff)


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
