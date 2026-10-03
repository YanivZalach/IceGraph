from pathlib import PurePosixPath
from typing import List

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from base_classes.base_file import BaseFile
from env import Env
from extractors.extractor import Extractor

PARTITION_STATISTICS_FILE_FORMATS = {"parquet", "orc", "avro"}
DEFAULT_PARTITION_STATISTICS_FILE_FORMAT = "parquet"


class PartitionStatisticsExtractor(Extractor):
    def __init__(self, table_name: str, partition_statistics_files: List[BaseFile]):
        super().__init__(table_name)
        self._partition_statistics_files = partition_statistics_files

    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        summaries_df = None
        samples_df = None
        for statistics_file in self._partition_statistics_files:
            source_df = self._read_source(statistics_file, lambda: self._read_partition_statistics_file(statistics_file.file_path))
            if source_df is None:
                continue

            columns = source_df.columns
            summary_df = self._summarize_partition_statistics(source_df, columns).withColumn("file_path", F.lit(statistics_file.file_path))
            sample_df = self._sample_partitions(source_df, columns).withColumn("file_path", F.lit(statistics_file.file_path))
            summaries_df = summary_df if summaries_df is None else summaries_df.unionByName(summary_df, allowMissingColumns=True)
            samples_df = sample_df if samples_df is None else samples_df.unionByName(sample_df, allowMissingColumns=True)

        if summaries_df is None:
            return self._spark.createDataFrame([], StructType([]))

        return summaries_df.join(samples_df, on="file_path", how="left")

    def _read_partition_statistics_file(self, file_path: str) -> pyspark.sql.DataFrame:
        file_format = PurePosixPath(file_path).suffix.lstrip(".").lower()
        if file_format not in PARTITION_STATISTICS_FILE_FORMATS:
            file_format = DEFAULT_PARTITION_STATISTICS_FILE_FORMAT

        return self._spark.read.format(file_format).load(file_path)

    @staticmethod
    def _summarize_partition_statistics(partition_statistics_df: pyspark.sql.DataFrame, columns: List[str]) -> pyspark.sql.DataFrame:
        if "position_delete_file_count" not in columns:
            partition_statistics_df = partition_statistics_df.withColumn("position_delete_file_count", F.lit(0))
        if "equality_delete_file_count" not in columns:
            partition_statistics_df = partition_statistics_df.withColumn("equality_delete_file_count", F.lit(0))

        partitions_with_deletes = F.count(
            F.when(
                (F.coalesce(F.col("position_delete_file_count"), F.lit(0)) > 0) | (F.coalesce(F.col("equality_delete_file_count"), F.lit(0)) > 0),
                1,
            )
        )

        return partition_statistics_df.agg(
            F.struct(
                F.count("*").alias("partitions_count"),
                partitions_with_deletes.alias("partitions_with_deletes"),
                PartitionStatisticsExtractor._partition_distribution().alias("partition_distribution"),
            ).alias("summary")
        )

    @staticmethod
    def _sample_partitions(partition_statistics_df: pyspark.sql.DataFrame, columns: List[str]) -> pyspark.sql.DataFrame:
        sample_df = partition_statistics_df
        if "last_updated_at" in columns:
            sample_df = sample_df.orderBy(F.desc_nulls_last("last_updated_at"))

        sample_df = sample_df.limit(Env.MAX_PARTITION_STATISTICS_ROWS).select(F.struct("*").alias("partition_row"))
        return sample_df.agg(F.collect_list("partition_row").alias("sampled_partitions"))

    @staticmethod
    def _partition_distribution() -> pyspark.sql.Column:
        distribution = [
            PartitionStatisticsExtractor._min_avg_max(F.col(column)).alias(column)
            for column in ["data_record_count", "total_data_file_size_in_bytes", "data_file_count"]
        ]

        average_data_file_size = F.when(F.col("data_file_count") > 0, F.col("total_data_file_size_in_bytes") / F.col("data_file_count"))
        distribution.append(PartitionStatisticsExtractor._min_avg_max(average_data_file_size).alias("average_data_file_size_in_bytes"))

        return F.struct(*distribution)

    @staticmethod
    def _min_avg_max(value: pyspark.sql.Column) -> pyspark.sql.Column:
        return F.struct(F.min(value).alias("min"), F.avg(value).alias("avg"), F.max(value).alias("max"))
