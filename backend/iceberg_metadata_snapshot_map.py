from typing import Dict
import arrow
from datetime import timezone
from spark_connect import open_spark_connect_session

from constants import MAX_SNAPSHOTS_TO_SHOW


def collect_snapshot_map(full_table_name: str) -> Dict[str, str]:
    spark = open_spark_connect_session()

    spark_tz = spark.conf.get("spark.sql.session.timeZone")

    df = spark.sql(f"""
        SELECT
            date_format(committed_at, "yyyy-MM-dd'T'HH:mm:ss.SSS") AS snapshot_timestamp,
            snapshot_id
        FROM {full_table_name}.snapshots
        ORDER BY committed_at DESC
    """).limit(MAX_SNAPSHOTS_TO_SHOW)

    return {
        arrow.get(row.snapshot_timestamp)
        .replace(tzinfo=spark_tz)
        .isoformat(): str(row.snapshot_id)
        for row in df.collect()
    }
