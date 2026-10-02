from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyspark.sql
from pyspark.sql import Window, functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType

from base_classes.base_file import BaseFile
from constants import FileType

TABLE_STATISTICS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("statistics-path", StringType()),
            StructField("file-size-in-bytes", LongType()),
            StructField("file-footer-size-in-bytes", LongType()),
            StructField("key-metadata", StringType()),
            StructField(
                "blob-metadata",
                ArrayType(
                    StructType(
                        [
                            StructField("type", StringType()),
                            StructField("snapshot-id", LongType()),
                            StructField("sequence-number", LongType()),
                            StructField("fields", ArrayType(IntegerType())),
                            StructField("properties", MapType(StringType(), StringType())),
                        ]
                    )
                ),
            ),
        ]
    )
)


@dataclass
class TableStatisticsFileRecord(BaseFile):
    snapshot_id: int
    file_size_in_bytes: str
    file_footer_size_in_bytes: str
    key_metadata: Optional[str]
    blobs: List[Dict[str, Any]]


def attach_added_table_statistics(metadata_files_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    if "statistics" not in metadata_files_df.columns:
        metadata_files_df = metadata_files_df.withColumn("statistics", F.lit(None).cast(StringType()))

    metadata_columns = [column for column in metadata_files_df.columns if column != "statistics"]
    listings_df = metadata_files_df.select(
        "file",
        "metadata_timestamp",
        "is_before_range",
        F.struct(*metadata_columns).alias("metadata_row"),
        F.explode_outer(F.from_json("statistics", TABLE_STATISTICS_SCHEMA)).alias("statistics_entry"),
    ).withColumn("statistics_path", F.col("statistics_entry").getField("statistics-path"))

    first_listing = Window.partitionBy("statistics_path").orderBy(F.desc("is_before_range"), "metadata_timestamp")
    is_added = (F.row_number().over(first_listing) == 1) & ~F.col("is_before_range") & F.col("statistics_path").isNotNull()

    return (
        listings_df.withColumn("is_added", is_added)
        .filter(~F.col("is_before_range"))
        .groupBy("file")
        .agg(
            F.first("metadata_row").alias("metadata_row"),
            F.collect_list(F.when(F.col("is_added"), F.col("statistics_entry"))).alias("added_statistics"),
        )
        .select("metadata_row.*", "added_statistics")
    )


def parse_table_statistics_entry(entry: dict) -> TableStatisticsFileRecord:
    return TableStatisticsFileRecord(
        type=FileType.TABLE_STATISTICS,
        file_path=entry["statistics-path"],
        child_files=[],
        snapshot_id=entry["snapshot-id"],
        file_size_in_bytes=str(entry["file-size-in-bytes"]),
        file_footer_size_in_bytes=str(entry["file-footer-size-in-bytes"]),
        key_metadata=entry["key-metadata"],
        blobs=[
            {
                "type": blob["type"],
                "fields": blob["fields"],
                "snapshot_id": blob["snapshot-id"],
                "sequence_number": blob["sequence-number"],
                "properties": blob["properties"] or {},
            }
            for blob in entry["blob-metadata"]
        ],
    )
