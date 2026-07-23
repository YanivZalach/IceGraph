from base_classes.utils import timed
from typing import Dict

from base_classes.utils import column_to_string_utc, to_arrow_utc

from spark_connect import open_spark_connect_session


@timed
def collect_snapshot_map(full_table_name: str, max_snapshots_to_show: int) -> Dict[str, Dict[str, str]]:
    spark = open_spark_connect_session()

    df = spark.sql(
        f"""
        SELECT
            committed_at AS snapshot_timestamp,
            snapshot_id,
            operation
        FROM {full_table_name}.snapshots
        ORDER BY committed_at DESC
    """
    )

    df = df.withColumn("snapshot_timestamp", column_to_string_utc("snapshot_timestamp")).limit(max_snapshots_to_show)

    return {
        to_arrow_utc(row.snapshot_timestamp).isoformat(): {"snapshot_id": str(row.snapshot_id), "operation": row.operation} for row in df.collect()
    }
