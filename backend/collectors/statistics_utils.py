from typing import List

import pyspark.sql
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType

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
