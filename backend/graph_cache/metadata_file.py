from typing import Optional

from pyspark.sql import functions as F

from spark_connect import open_spark_connect_session


def collect_graph_metadata_file(table_name: str, end_snapshot_id: Optional[int]) -> str:
    spark = open_spark_connect_session()
    metadata_entries = spark.sql(f"SELECT timestamp, file, latest_snapshot_id FROM {table_name}.metadata_log_entries")

    if end_snapshot_id is not None:
        metadata_entries = metadata_entries.filter(F.col("latest_snapshot_id") == end_snapshot_id)

    selected_entry = metadata_entries.orderBy(F.desc("timestamp")).select("file").first()

    if not selected_entry:
        if end_snapshot_id is None:
            raise ValueError(f"No metadata files found for table {table_name}")
        raise ValueError(f"No metadata file found for table {table_name} at snapshot {end_snapshot_id}")

    return selected_entry.file
