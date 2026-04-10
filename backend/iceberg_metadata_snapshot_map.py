from typing import Dict
from datetime import timezone
import pytz
from spark_connect import open_spark_connect_session

from constants import MAX_SNAPSHOTS_TO_SHOW


def collect_snapshot_map(full_table_name: str) -> Dict[str, str]:
    spark = open_spark_connect_session()

    tz = spark.conf.get("spark.sql.session.timeZone")
    tzinfo = pytz.timezone(tz)

    df = spark.sql(f"""
        SELECT
            committed_at AS snapshot_timestamp,
            snapshot_id
        FROM {full_table_name}.snapshots
        ORDER BY committed_at DESC
    """).limit(MAX_SNAPSHOTS_TO_SHOW)

    return {
        row.snapshot_timestamp.replace(tzinfo=tzinfo).isoformat(): str(row.snapshot_id)
        for row in df.collect()
    }
