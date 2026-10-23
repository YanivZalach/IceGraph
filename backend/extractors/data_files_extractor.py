from typing import List

import pyspark
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, IntegerType, LongType, MapType, StringType, StructField, StructType

from collectors.collect_manifests import ManifestRecord
from env import Env
from extractors.extractor import Extractor

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
                    StructField("column_sizes", MapType(IntegerType(), LongType()), True),
                    StructField("value_counts", MapType(IntegerType(), LongType()), True),
                    StructField("null_value_counts", MapType(IntegerType(), LongType()), True),
                    StructField("nan_value_counts", MapType(IntegerType(), LongType()), True),
                    StructField("lower_bounds", MapType(IntegerType(), BinaryType()), True),
                    StructField("upper_bounds", MapType(IntegerType(), BinaryType()), True),
                ]
            ),
            True,
        ),
        StructField("manifest_path", StringType(), False),
        StructField("added_snapshot_timestamp", StringType(), True),
        StructField("added_snapshot_id", LongType(), True),
        StructField("stored_first_row_id", LongType(), True),
        StructField("row_id_offset", LongType(), True),
        StructField("referenced_data_file", StringType(), True),
        StructField("content_offset", LongType(), True),
        StructField("content_size_in_bytes", LongType(), True),
        StructField("node_id", StringType(), True),
    ]
)

DATA_FILE_CONTENT_DATA = 0
MANIFEST_ENTRY_STATUS_DELETED = 2


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

        cutoff_snapshot_df = self._find_cutoff_snapshot(data_files_limited_df)

        return self._mark_included_data_files(data_files_limited_df, cutoff_snapshot_df)

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
        df = self._spark.read.format("avro").load(manifest_entry.file_path).select("status", "data_file")
        existing_fields = set(df.schema["data_file"].dataType.fieldNames())
        for field in DATA_FILE_RECORD_SCHEMA["data_file"].dataType.fields:
            if field.name in existing_fields:
                continue

            if field.name == "content":
                default_value = F.lit(0).cast(IntegerType())
            else:
                default_value = F.lit(None)

            data_file = F.col("data_file").withField(field.name, default_value)
            df = df.withColumn("data_file", data_file)

        has_row_id_assignments = bool(manifest_entry.hidden_manifest_data.row_id_assignments)
        row_id_df = self._with_row_id_inheritance(df, existing_fields, has_row_id_assignments)
        data_files_df = self._with_deletion_vector_columns(row_id_df, existing_fields)

        return (
            data_files_df.select(
                "status",
                "data_file",
                "stored_first_row_id",
                "row_id_offset",
                "referenced_data_file",
                "content_offset",
                "content_size_in_bytes",
                "node_id",
            )
            .withColumn("manifest_path", F.lit(manifest_entry.file_path))
            .withColumn(
                "added_snapshot_timestamp",
                F.lit(manifest_entry.added_snapshot_timestamp),
            )
            .withColumn("added_snapshot_id", F.lit(manifest_entry.added_snapshot_id))
        )

    @staticmethod
    def _with_row_id_inheritance(manifest_df, data_file_fields: set[str], has_row_id_assignments: bool):
        if not has_row_id_assignments:
            return manifest_df.withColumn("stored_first_row_id", F.lit(None).cast(LongType())).withColumn(
                "row_id_offset", F.lit(None).cast(LongType())
            )

        stored_first_row_id = F.col("data_file.first_row_id") if "first_row_id" in data_file_fields else F.lit(None).cast(LongType())
        is_data_file = F.col("data_file.content") == DATA_FILE_CONTENT_DATA
        is_live_data_file = is_data_file & (F.col("status") != MANIFEST_ENTRY_STATUS_DELETED)

        preceding_entries = Window.orderBy("manifest_entry_position").rowsBetween(Window.unboundedPreceding, -1)
        unassigned_record_count = F.when(is_live_data_file & stored_first_row_id.isNull(), F.col("data_file.record_count")).otherwise(F.lit(0))
        preceding_unassigned_rows = F.coalesce(F.sum(unassigned_record_count).over(preceding_entries), F.lit(0))

        row_id_offset = F.when(is_live_data_file & stored_first_row_id.isNull(), preceding_unassigned_rows)

        return (
            manifest_df.withColumn("manifest_entry_position", F.monotonically_increasing_id())
            .withColumn("stored_first_row_id", F.when(is_data_file, stored_first_row_id))
            .withColumn("row_id_offset", row_id_offset)
        )

    @staticmethod
    def _with_deletion_vector_columns(data_files_df, data_file_fields: set[str]):
        def data_file_field(name, data_type):
            return F.col(f"data_file.{name}") if name in data_file_fields else F.lit(None).cast(data_type)

        content_offset = data_file_field("content_offset", LongType())
        node_id = F.when(content_offset.isNull(), F.col("data_file.file_path")).otherwise(
            F.concat(F.col("data_file.file_path"), F.lit("#"), content_offset.cast(StringType()))
        )

        return (
            data_files_df.withColumn("referenced_data_file", data_file_field("referenced_data_file", StringType()))
            .withColumn("content_offset", content_offset)
            .withColumn("content_size_in_bytes", data_file_field("content_size_in_bytes", LongType()))
            .withColumn("node_id", node_id)
        )

    @staticmethod
    def _match_data_file_to_latest_snapshot(avro_df):
        window_desc = Window.partitionBy("node_id").orderBy(F.desc("added_snapshot_timestamp"))
        window_asc = Window.partitionBy("node_id").orderBy(F.asc("added_snapshot_timestamp"))

        avro_df = (
            avro_df.withColumn("row_num", F.row_number().over(window_desc))
            .withColumn("earliest_snapshot_timestamp", F.first("added_snapshot_timestamp").over(window_asc))
            .withColumn("earliest_snapshot_id", F.first("added_snapshot_id").over(window_asc))
        )

        latest_df = avro_df.filter(F.col("row_num") == 1).select(
            F.col("node_id"),
            F.col("data_file"),
            F.col("referenced_data_file"),
            F.col("content_offset"),
            F.col("content_size_in_bytes"),
            F.col("added_snapshot_timestamp").alias("latest_snapshot_timestamp"),
            F.col("added_snapshot_id").alias("latest_snapshot_id"),
            F.col("earliest_snapshot_timestamp"),
            F.col("earliest_snapshot_id"),
        )
        return latest_df

    @staticmethod
    def _group_data_files_by_manifests(avro_df):
        manifest_entries_df = avro_df.groupBy("node_id").agg(
            F.collect_list(
                F.struct(
                    F.col("manifest_path").alias("path"),
                    F.col("status").alias("status"),
                )
            ).alias("pointing_manifests"),
            F.collect_list(
                F.struct(
                    F.col("manifest_path").alias("path"),
                    F.col("stored_first_row_id").alias("stored_first_row_id"),
                    F.col("row_id_offset").alias("row_id_offset"),
                )
            ).alias("row_id_entries"),
        )
        return manifest_entries_df

    @staticmethod
    def _join_data_file_with_manifest_entries(latest_df, manifest_entries_df):
        return manifest_entries_df.join(latest_df, on="node_id", how="inner").select(
            "node_id",
            "referenced_data_file",
            "content_offset",
            "content_size_in_bytes",
            "pointing_manifests",
            "row_id_entries",
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
            "data_file.column_sizes",
            "data_file.value_counts",
            "data_file.null_value_counts",
            "data_file.nan_value_counts",
            "data_file.lower_bounds",
            "data_file.upper_bounds",
        )

    @staticmethod
    def _limit_and_rank_files_by_snapshot_timestamp(df):
        df = df.orderBy(F.desc("latest_snapshot_timestamp")).limit(Env.MAX_DATA_FILES_TO_COLLECT + 1)

        row_num_window = Window.orderBy(F.desc("latest_snapshot_timestamp"))
        df = df.withColumn("row_num", F.row_number().over(row_num_window))

        return df

    @staticmethod
    def _find_cutoff_snapshot(df):
        return df.filter(F.col("row_num") == Env.MAX_DATA_FILES_TO_COLLECT + 1).agg(
            (F.count("*") > 0).alias("data_files_cutoff_reached"),
            F.first("latest_snapshot_id").alias("cutoff_snapshot_id"),
            F.first("latest_snapshot_timestamp").alias("cutoff_snapshot_timestamp"),
        )

    @staticmethod
    def _mark_included_data_files(grouped_files_limited_df, cutoff_snapshot_df):
        return (
            grouped_files_limited_df.join(F.broadcast(cutoff_snapshot_df), how="cross")
            .withColumn(
                "included",
                ~F.col("data_files_cutoff_reached")
                | (F.col("latest_snapshot_timestamp") > F.coalesce(F.col("cutoff_snapshot_timestamp"), F.lit(0).cast("timestamp"))),
            )
            .filter(F.col("included") | (F.col("row_num") == Env.MAX_DATA_FILES_TO_COLLECT + 1))
            .drop("row_num", "latest_snapshot_timestamp", "latest_snapshot_id")
        )
