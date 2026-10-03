from functools import reduce
from typing import List

import pyspark.sql
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from base_classes.base_file import BaseFile
from env import Env
from extractors.extractor import Extractor

PARTITION_STATISTICS_FILE_FORMATS = {"parquet", "orc", "avro"}
DEFAULT_PARTITION_STATISTICS_FILE_FORMAT = "parquet"
SAMPLE_ORDER_COLUMN = "last_updated_at"
RECORD_COUNT_COLUMN = "data_record_count"
DATA_FILE_COUNT_COLUMN = "data_file_count"
DATA_FILE_SIZE_COLUMN = "total_data_file_size_in_bytes"
AVERAGE_DATA_FILE_SIZE = "average_data_file_size_in_bytes"
DELETE_FILE_COUNT_COLUMNS = ["position_delete_file_count", "equality_delete_file_count"]


class PartitionStatisticsExtractor(Extractor):
    def __init__(self, table_name: str, partition_statistics_files: List[BaseFile]):
        super().__init__(table_name)
        self._partition_statistics_files = partition_statistics_files

    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        statistics_df = None
        for index, statistics_file in enumerate(self._partition_statistics_files):
            source_df = self._read_source(statistics_file, lambda: self._read_partition_statistics_file(statistics_file.file_path))
            if source_df is None:
                continue

            summary_df = self._summarize_partition_statistics(source_df).select(
                F.struct(
                    F.lit(statistics_file.file_path).alias("file_path"),
                    F.col("partitions_count"),
                    F.col("partition_distribution"),
                    F.col("sampled_partitions"),
                ).alias(f"file_{index}")
            )
            statistics_df = summary_df if statistics_df is None else statistics_df.crossJoin(summary_df)

        return statistics_df if statistics_df is not None else self._spark.createDataFrame([], StructType([]))

    def _read_partition_statistics_file(self, file_path: str) -> pyspark.sql.DataFrame:
        file_format = file_path.rsplit(".", 1)[-1].lower()
        if file_format not in PARTITION_STATISTICS_FILE_FORMATS:
            file_format = DEFAULT_PARTITION_STATISTICS_FILE_FORMAT

        return self._spark.read.format(file_format).load(file_path)

    @staticmethod
    def _summarize_partition_statistics(partition_statistics_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        columns = partition_statistics_df.columns
        summary_df = partition_statistics_df.agg(
            F.count("*").alias("partitions_count"),
            PartitionStatisticsExtractor._partition_distribution(columns).alias("partition_distribution"),
        )
        sample_df = partition_statistics_df
        has_update_time = SAMPLE_ORDER_COLUMN in columns
        if has_update_time:
            sample_df = sample_df.orderBy(F.desc_nulls_last(SAMPLE_ORDER_COLUMN))

        sample_df = sample_df.limit(Env.MAX_PARTITION_STATISTICS_ROWS).select(
            F.struct(*[F.col(f"`{column.replace('`', '``')}`") for column in columns]).alias("partition_row")
        )
        sample_order = F.col("partition_row").getField(SAMPLE_ORDER_COLUMN).desc_nulls_last() if has_update_time else F.lit(0)
        sample_df = sample_df.withColumn("sample_rank", F.row_number().over(Window.orderBy(sample_order)))
        samples_df = sample_df.agg(
            F.transform(
                F.array_sort(
                    F.collect_list(F.struct("sample_rank", "partition_row")),
                    lambda left, right: left["sample_rank"] - right["sample_rank"],
                ),
                lambda sample: sample["partition_row"],
            ).alias("sampled_partitions")
        )

        return summary_df.crossJoin(samples_df)

    @staticmethod
    def _partition_distribution(columns: List[str]) -> pyspark.sql.Column:
        distribution = [
            PartitionStatisticsExtractor._min_avg_max(F.col(column)).alias(column)
            for column in [RECORD_COUNT_COLUMN, DATA_FILE_SIZE_COLUMN, DATA_FILE_COUNT_COLUMN]
            if column in columns
        ]

        if DATA_FILE_SIZE_COLUMN in columns and DATA_FILE_COUNT_COLUMN in columns:
            average_data_file_size = F.when(F.col(DATA_FILE_COUNT_COLUMN) > 0, F.col(DATA_FILE_SIZE_COLUMN) / F.col(DATA_FILE_COUNT_COLUMN))
            distribution.append(PartitionStatisticsExtractor._min_avg_max(average_data_file_size).alias(AVERAGE_DATA_FILE_SIZE))

        delete_file_counts = [F.coalesce(F.col(column), F.lit(0)) for column in DELETE_FILE_COUNT_COLUMNS if column in columns]
        if delete_file_counts:
            has_deletes = reduce(lambda total, count: total + count, delete_file_counts) > 0
            distribution.append(F.sum(F.when(has_deletes, 1).otherwise(0)).alias("partitions_with_deletes"))

        return F.struct(*distribution) if distribution else F.lit(None)

    @staticmethod
    def _min_avg_max(value: pyspark.sql.Column) -> pyspark.sql.Column:
        return F.struct(F.min(value).alias("min"), F.avg(value).alias("avg"), F.max(value).alias("max"))
