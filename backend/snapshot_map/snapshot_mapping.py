from base_classes.utils import timed
from typing import Any, Dict, Optional

import pyspark.sql
from pyspark.sql import functions as F

from base_classes.utils import column_to_string_utc, to_arrow_utc

from spark_connect import open_spark_connect_session


@timed
def collect_snapshot_map(table_name: str, max_snapshots_to_show: int, before_snapshot_id: Optional[int] = None) -> Dict[str, Any]:
    spark = open_spark_connect_session()

    snapshots_df = spark.sql(f"""
        SELECT
            committed_at AS snapshot_timestamp,
            snapshot_id,
            operation
        FROM {table_name}.snapshots
    """)

    if before_snapshot_id is not None:
        _validate_snapshot_exists(snapshots_df, before_snapshot_id)
        snapshots_df = _filter_older_than_snapshot(snapshots_df, before_snapshot_id)

    df = (
        snapshots_df.orderBy(F.col("snapshot_timestamp").desc(), F.col("snapshot_id").desc())
        .withColumn("snapshot_timestamp", column_to_string_utc("snapshot_timestamp"))
        .limit(max_snapshots_to_show + 1)
    )

    rows = df.collect()
    page_rows = rows[:max_snapshots_to_show]
    has_older_snapshots = len(rows) > max_snapshots_to_show
    next_before_snapshot_id = str(page_rows[-1].snapshot_id) if has_older_snapshots and page_rows else None

    return {
        "snapshots": {
            to_arrow_utc(row.snapshot_timestamp).isoformat(): {"snapshot_id": str(row.snapshot_id), "operation": row.operation} for row in page_rows
        },
        "next_before_snapshot_id": next_before_snapshot_id,
    }


def _validate_snapshot_exists(snapshots_df: pyspark.sql.DataFrame, snapshot_id: int) -> None:
    if snapshots_df.filter(F.col("snapshot_id") == snapshot_id).count() == 0:
        raise ValueError(f"Snapshot {snapshot_id} was not found in the table history. It may have expired.")


def _filter_older_than_snapshot(snapshots_df: pyspark.sql.DataFrame, before_snapshot_id: int) -> pyspark.sql.DataFrame:
    before_snapshot_df = snapshots_df.filter(F.col("snapshot_id") == before_snapshot_id).select(
        F.col("snapshot_timestamp").alias("before_snapshot_timestamp")
    )

    return (
        snapshots_df.crossJoin(before_snapshot_df)
        .filter(
            (F.col("snapshot_timestamp") < F.col("before_snapshot_timestamp"))
            | ((F.col("snapshot_timestamp") == F.col("before_snapshot_timestamp")) & (F.col("snapshot_id") < before_snapshot_id))
        )
        .drop("before_snapshot_timestamp")
    )
