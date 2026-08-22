from typing import List

import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from collectors.collect_manifests import ManifestRecord
from env import Env
from extractors.extractor import Extractor
from icegraph_logger import logger

DATA_FILE_RECORD_SCHEMA = StructType(
    [
        StructField("status", StringType(), False),
        StructField(
            "data_file",
            StructType(
                [
                    StructField("file_path", StringType(), True),
                    StructField("content", LongType(), True),
                    StructField("file_format", StringType(), True),
                    StructField("file_size_in_bytes", LongType(), True),
                    StructField("record_count", LongType(), True),
                    StructField("partition", StringType(), True),
                    StructField("sort_order_id", LongType(), True),
                    StructField("split_offsets", StringType(), True),
                    StructField("key_metadata", StringType(), True),
                    StructField("equality_ids", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("manifest_path", StringType(), False),
        StructField("added_snapshot_timestamp", StringType(), True),
        StructField("added_snapshot_id", LongType(), True),
    ]
)


class DataFilesExtractor(Extractor):
    def __init__(self, table_name: str, manifest_entries: List[ManifestRecord]):
        super().__init__(table_name)
        self._manifest_entries = manifest_entries

    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        data_files_df = self._collect_data_files_from_manifests(self._manifest_entries)
        data_files_with_latest_ts_df = self._match_data_file_to_latest_snapshot(data_files_df)

        data_files_by_manifests_df = self._group_data_files_by_manifests(data_files_df)

        data_files_with_manifest_entries_df = self._join_data_file_with_manifest_entries(data_files_with_latest_ts_df, data_files_by_manifests_df)
        data_files_limited_df = self._limit_and_rank_files_by_snapshot_timestamp(data_files_with_manifest_entries_df)

        snapshot_timestamp_cutoff_df = self._find_cutoff_snapshot_timestamp(data_files_limited_df)

        included_data_files_df = self._find_included_data_files(data_files_limited_df, snapshot_timestamp_cutoff_df)

        return self._enrich_data_files_with_summary(included_data_files_df)

    def _collect_data_files_from_manifests(self, manifest_rows):
        avro_df = None
        for manifest_entry in manifest_rows:
            df = self._read_source(manifest_entry, lambda: self._collect_data_files_from_manifest(manifest_entry))
            if df is None:
                continue

            if avro_df is None:
                avro_df = df
            else:
                avro_df = avro_df.unionByName(df, allowMissingColumns=True)

        if avro_df is None:
            return self._spark.createDataFrame([], DATA_FILE_RECORD_SCHEMA)

        return avro_df

    def _collect_data_files_from_manifest(self, manifest_entry: ManifestRecord):
        return (
            self._spark.read.format("avro")
            .load(manifest_entry.file_path)
            .select("status", "data_file")
            .withColumn("manifest_path", F.lit(manifest_entry.file_path))
            .withColumn(
                "added_snapshot_timestamp",
                F.lit(manifest_entry.added_snapshot_timestamp),
            )
            .withColumn("added_snapshot_id", F.lit(manifest_entry.added_snapshot_id))
        )

    @staticmethod
    def _match_data_file_to_latest_snapshot(avro_df):
        window_desc = Window.partitionBy("data_file.file_path").orderBy(F.desc("added_snapshot_timestamp"))
        window_asc = Window.partitionBy("data_file.file_path").orderBy(F.asc("added_snapshot_timestamp"))

        avro_df = (
            avro_df.withColumn("row_num", F.row_number().over(window_desc))
            .withColumn("earliest_snapshot_timestamp", F.first("added_snapshot_timestamp", ignorenulls=True).over(window_asc))
            .withColumn("earliest_snapshot_id", F.first("added_snapshot_id", ignorenulls=True).over(window_asc))
        )

        latest_df = avro_df.filter(F.col("row_num") == 1).select(
            F.col("data_file.file_path").alias("file_path"),
            F.col("data_file"),
            F.col("added_snapshot_timestamp").alias("latest_snapshot_timestamp"),
            F.col("added_snapshot_id").alias("latest_snapshot_id"),
            F.col("earliest_snapshot_timestamp"),
            F.col("earliest_snapshot_id"),
        )
        return latest_df

    @staticmethod
    def _group_data_files_by_manifests(avro_df):
        manifest_entries_df = avro_df.groupBy("data_file.file_path").agg(
            F.collect_list(
                F.struct(
                    F.col("manifest_path").alias("path"),
                    F.col("status").alias("status"),
                )
            ).alias("pointing_manifests")
        )
        return manifest_entries_df

    @staticmethod
    def _join_data_file_with_manifest_entries(latest_df, manifest_entries_df):
        return manifest_entries_df.join(latest_df, on="file_path", how="inner").select(
            "pointing_manifests",
            "latest_snapshot_id",
            "latest_snapshot_timestamp",
            "earliest_snapshot_id",
            "earliest_snapshot_timestamp",
            "data_file.file_path",
            "data_file.content",
            "data_file.file_format",
            "data_file.file_size_in_bytes",
            "data_file.record_count",
            "data_file.partition",
            "data_file.sort_order_id",
            "data_file.split_offsets",
            "data_file.key_metadata",
            "data_file.equality_ids",
        )

    @staticmethod
    def _limit_and_rank_files_by_snapshot_timestamp(df):
        df = df.orderBy(F.desc("latest_snapshot_timestamp")).limit(Env.MAX_DATA_FILES_TO_COLLECT + 1)

        row_num_window = Window.orderBy(F.desc("latest_snapshot_timestamp"))
        df = df.withColumn("row_num", F.row_number().over(row_num_window))

        return df

    @staticmethod
    def _find_cutoff_snapshot_timestamp(df):
        return (
            df.filter(F.col("row_num") == Env.MAX_DATA_FILES_TO_COLLECT + 1)
            .agg(F.coalesce(F.first("latest_snapshot_timestamp"), F.lit(0).cast("timestamp")).alias("snapshot_timestamp_cutoff"))
            .select("snapshot_timestamp_cutoff")
        )

    @staticmethod
    def _find_included_data_files(grouped_files_limited_df, snapshot_timestamp_cutoff_df):
        return (
            grouped_files_limited_df.join(F.broadcast(snapshot_timestamp_cutoff_df), how="cross")
            .filter(F.col("latest_snapshot_timestamp") > F.col("snapshot_timestamp_cutoff"))
            .drop("row_num", "snapshot_timestamp_cutoff", "latest_snapshot_timestamp", "latest_snapshot_id")
        )

    def _enrich_data_files_with_summary(self, data_files_df):
        try:
            summaries_df = self._spark.sql(f"SELECT file_path, readable_metrics AS summary FROM {self._table_name}.all_files").dropDuplicates(
                ["file_path"]
            )
            summaries_df.schema
        except Exception:
            logger.warning(f"[{self._table_name}] Failed to enrich data files from all_files", exc_info=True)
            return data_files_df.withColumn("summary", F.lit(None))

        return data_files_df.join(summaries_df, on="file_path", how="left")
