from concurrent.futures import ThreadPoolExecutor
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
MANIFEST_READ_WORKERS = 8


class DataFilesExtractor(Extractor):
    def __init__(self, table_name: str, manifest_entries: List[ManifestRecord]):
        super().__init__(table_name)
        self._manifest_entries = manifest_entries

    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        manifest_entries_df = self._collect_data_files_from_manifests(self._manifest_entries)
        data_files_df = self._group_entries_by_data_file(manifest_entries_df)

        return self._select_newest_data_files(data_files_df)

    def _collect_data_files_from_manifests(self, manifest_rows):
        with ThreadPoolExecutor(max_workers=MANIFEST_READ_WORKERS) as executor:
            manifest_dfs = list(executor.map(self._read_manifest, manifest_rows))

        avro_df = None
        for df in manifest_dfs:
            if df is None:
                continue

            if avro_df is None:
                avro_df = df
            else:
                avro_df = avro_df.unionByName(df, allowMissingColumns=True)

        if avro_df is None:
            return self._spark.createDataFrame([], DATA_FILE_RECORD_SCHEMA)

        return avro_df

    def _read_manifest(self, manifest_entry: ManifestRecord):
        return self._read_source(manifest_entry, lambda: self._collect_data_files_from_manifest(manifest_entry))

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
        inherits_row_id = is_live_data_file & stored_first_row_id.isNull()

        preceding_entries = Window.orderBy("manifest_entry_position").rowsBetween(Window.unboundedPreceding, -1)
        unassigned_record_count = F.when(inherits_row_id, F.col("data_file.record_count")).otherwise(F.lit(0))
        preceding_unassigned_rows = F.coalesce(F.sum(unassigned_record_count).over(preceding_entries), F.lit(0))

        row_id_offset = F.when(inherits_row_id, preceding_unassigned_rows)

        return (
            manifest_df.withColumn("manifest_entry_position", F.monotonically_increasing_id())
            .withColumn("stored_first_row_id", F.when(is_data_file, stored_first_row_id))
            .withColumn("row_id_offset", row_id_offset)
        )

    @staticmethod
    def _with_deletion_vector_columns(data_files_df, data_file_fields: set[str]):
        def _data_file_field(name, data_type):
            return F.col(f"data_file.{name}") if name in data_file_fields else F.lit(None).cast(data_type)

        content_offset = _data_file_field("content_offset", LongType())
        node_id = F.when(content_offset.isNull(), F.col("data_file.file_path")).otherwise(
            F.concat(F.col("data_file.file_path"), F.lit("#"), content_offset.cast(StringType()))
        )

        return (
            data_files_df.withColumn("referenced_data_file", _data_file_field("referenced_data_file", StringType()))
            .withColumn("content_offset", content_offset)
            .withColumn("content_size_in_bytes", _data_file_field("content_size_in_bytes", LongType()))
            .withColumn("node_id", node_id)
        )

    @staticmethod
    def _group_entries_by_data_file(manifest_entries_df):
        added_snapshot_timestamp = F.col("added_snapshot_timestamp")
        timestamp_with_nulls_first = F.struct(added_snapshot_timestamp.isNotNull(), added_snapshot_timestamp)

        latest_entry = F.struct(
            "data_file",
            "referenced_data_file",
            "content_offset",
            "content_size_in_bytes",
            F.col("added_snapshot_id").alias("snapshot_id"),
            added_snapshot_timestamp.alias("snapshot_timestamp"),
        )
        earliest_entry = F.struct(
            F.col("added_snapshot_id").alias("snapshot_id"),
            added_snapshot_timestamp.alias("snapshot_timestamp"),
        )
        pointing_manifest = F.struct(
            F.col("manifest_path").alias("path"),
            F.col("status").alias("status"),
        )
        row_id_entry = F.struct(
            F.col("manifest_path").alias("path"),
            F.col("stored_first_row_id").alias("stored_first_row_id"),
            F.col("row_id_offset").alias("row_id_offset"),
        )

        return manifest_entries_df.groupBy("node_id").agg(
            F.max_by(latest_entry, timestamp_with_nulls_first).alias("latest_entry"),
            F.min_by(earliest_entry, timestamp_with_nulls_first).alias("earliest_entry"),
            F.collect_list(pointing_manifest).alias("pointing_manifests"),
            F.collect_list(row_id_entry).alias("row_id_entries"),
        )

    @staticmethod
    def _select_newest_data_files(data_files_df):
        return (
            data_files_df.select(
                "node_id",
                "latest_entry.referenced_data_file",
                "latest_entry.content_offset",
                "latest_entry.content_size_in_bytes",
                "pointing_manifests",
                "row_id_entries",
                F.col("latest_entry.snapshot_id").alias("latest_snapshot_id"),
                F.col("latest_entry.snapshot_timestamp").alias("latest_snapshot_timestamp"),
                F.col("earliest_entry.snapshot_id").alias("earliest_snapshot_id"),
                F.col("earliest_entry.snapshot_timestamp").alias("earliest_snapshot_timestamp"),
                "latest_entry.data_file.file_path",
                "latest_entry.data_file.content",
                "latest_entry.data_file.file_format",
                "latest_entry.data_file.file_size_in_bytes",
                "latest_entry.data_file.record_count",
                "latest_entry.data_file.partition",
                "latest_entry.data_file.sort_order_id",
                "latest_entry.data_file.split_offsets",
                "latest_entry.data_file.key_metadata",
                "latest_entry.data_file.equality_ids",
                "latest_entry.data_file.column_sizes",
                "latest_entry.data_file.value_counts",
                "latest_entry.data_file.null_value_counts",
                "latest_entry.data_file.nan_value_counts",
                "latest_entry.data_file.lower_bounds",
                "latest_entry.data_file.upper_bounds",
            )
            .orderBy(F.desc("latest_snapshot_timestamp"))
            .limit(Env.MAX_DATA_FILES_TO_COLLECT + 1)
        )
