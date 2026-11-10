from base_classes.utils import column_to_string_utc
from dataclasses import dataclass, field
from typing import Dict, List, NamedTuple, Optional

import arrow
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from constants import NO_REF_LEADS_TO_SNAPSHOT_REASON, SNAPSHOT_METADATA_WINDOW_WARNING
from icegraph_logger import logger
from metadata_log.metadata_log import MetadataLog, RefMetadataWindow
from base_classes.utils import timed, to_arrow_utc


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
    metadata_log = MetadataLog(spark, table_name)

    if start_snapshot_id:
        start = _get_start_cutoffs(spark, table_name, start_snapshot_id, metadata_log, cutoff.warnings)
        cutoff.start_snapshot_cutoff = start.snapshot_cutoff
        cutoff.start_metadata_cutoff = start.metadata_cutoff
        cutoff.manifests_to_ignore_df = start.manifests_to_ignore_df

    if end_snapshot_id:
        end = _get_end_cutoffs(spark, table_name, end_snapshot_id, metadata_log, cutoff.warnings)
        cutoff.end_snapshot_cutoff = end.snapshot_cutoff
        cutoff.end_metadata_cutoff = end.metadata_cutoff

    if cutoff.start_snapshot_cutoff > cutoff.end_snapshot_cutoff:
        raise ValueError("Start snapshot is after end snapshot")

    return cutoff


def _get_start_cutoffs(
    spark: SparkSession,
    table_name: str,
    start_snapshot_id: int,
    metadata_log: MetadataLog,
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

    meta_row = (
        spark.sql(f"""
        SELECT MIN(timestamp) AS ts
        FROM {table_name}.metadata_log_entries
        WHERE latest_snapshot_id = {start_snapshot_id}
    """)
        .withColumn("ts", column_to_string_utc("ts"))
        .first()
    )

    if meta_row and meta_row.ts:
        metadata_cutoff = to_arrow_utc(meta_row.ts)
    else:
        ref_window = _find_ref_window(metadata_log, table_name, start_snapshot_id, "start", warnings)
        metadata_cutoff = ref_window.first_entry.timestamp if ref_window else snapshot_cutoff

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
    metadata_log: MetadataLog,
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

    meta_row = (
        spark.sql(f"""
        SELECT MAX(timestamp) AS ts
        FROM {table_name}.metadata_log_entries
        WHERE latest_snapshot_id = {end_snapshot_id}
    """)
        .withColumn("ts", column_to_string_utc("ts"))
        .first()
    )

    if meta_row and meta_row.ts:
        metadata_cutoff = to_arrow_utc(meta_row.ts)
    else:
        ref_window = _find_ref_window(metadata_log, table_name, end_snapshot_id, "end", warnings)
        metadata_cutoff = ref_window.last_entry.timestamp if ref_window else snapshot_cutoff

    return EndCutoffs(
        snapshot_cutoff=snapshot_cutoff,
        metadata_cutoff=metadata_cutoff,
    )


def _find_ref_window(
    metadata_log: MetadataLog,
    table_name: str,
    snapshot_id: int,
    label: str,
    warnings: Dict[str, List[str]],
) -> Optional[RefMetadataWindow]:
    try:
        ref_window = metadata_log.find_ref_window(snapshot_id)
        reason = NO_REF_LEADS_TO_SNAPSHOT_REASON

    except Exception as e:
        logger.warning(f"[{table_name}] Failed to read the branches and tags from the current metadata file", exc_info=True)
        error_lines = str(e).strip().splitlines()
        ref_window = None
        reason = error_lines[0] if error_lines else type(e).__name__

    if ref_window is None:
        warnings[f"{label}_snapshot_metadata_window"] = [SNAPSHOT_METADATA_WINDOW_WARNING.format(label=label, snapshot_id=snapshot_id, reason=reason)]

    return ref_window


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
