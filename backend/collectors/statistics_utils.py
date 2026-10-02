from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyspark.sql
from pyspark.sql import SparkSession, functions as F
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

METADATA_FILE_STATISTICS_SCHEMA = StructType([StructField("statistics", TABLE_STATISTICS_SCHEMA)])


@dataclass
class TableStatisticsFileRecord(BaseFile):
    snapshot_id: int
    file_size_in_bytes: str
    file_footer_size_in_bytes: str
    key_metadata: Optional[str]
    blobs: List[Dict[str, Any]]


def with_pointed_table_statistics(metadata_files_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    if "statistics" not in metadata_files_df.columns:
        metadata_files_df = metadata_files_df.withColumn("statistics", F.lit(None).cast(StringType()))

    pointed_statistics = F.transform(
        F.from_json("statistics", TABLE_STATISTICS_SCHEMA),
        lambda entry: F.struct(
            entry.getField("snapshot-id").alias("snapshot_id"),
            entry.getField("statistics-path").alias("statistics_path"),
        ),
    )

    return metadata_files_df.withColumn("pointed_statistics", pointed_statistics).drop("statistics")


def read_table_statistics(spark: SparkSession, metadata_files: List[str]) -> pyspark.sql.DataFrame:
    statistics_df = None
    for metadata_file in metadata_files:
        df = (
            spark.read.schema(METADATA_FILE_STATISTICS_SCHEMA)
            .option("multiLine", True)
            .json(metadata_file)
            .select(F.lit(metadata_file).alias("file"), "statistics")
        )
        statistics_df = df if statistics_df is None else statistics_df.unionByName(df)

    return statistics_df


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
