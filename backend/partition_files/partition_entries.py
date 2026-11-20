from dataclasses import dataclass
from typing import Optional

import pyspark
from pyspark.sql import Column, Row
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BinaryType, IntegerType, LongType, StringType, StructField, StructType

from base_classes.utils import timed
from icegraph_logger import logger
from partition_files.constants import MANIFEST_ENTRY_STATUS_DELETED, PARTITION_MATCH_MATCHED, PARTITION_MATCH_UNDECIDED
from partition_files.manifest_pruning import ManifestCandidate
from spark_connect import open_spark_connect_session


def _metric_map_type(value_type) -> ArrayType:
    return ArrayType(StructType([StructField("key", IntegerType(), True), StructField("value", value_type, True)]))


DATA_FILE_COLUMNS = [
    ("content", IntegerType()),
    ("file_path", StringType()),
    ("file_format", StringType()),
    ("record_count", LongType()),
    ("file_size_in_bytes", LongType()),
    ("sort_order_id", IntegerType()),
    ("split_offsets", ArrayType(LongType())),
    ("equality_ids", ArrayType(IntegerType())),
    ("referenced_data_file", StringType()),
    ("content_offset", LongType()),
    ("content_size_in_bytes", LongType()),
    ("column_sizes", _metric_map_type(LongType())),
    ("value_counts", _metric_map_type(LongType())),
    ("null_value_counts", _metric_map_type(LongType())),
    ("nan_value_counts", _metric_map_type(LongType())),
    ("lower_bounds", _metric_map_type(BinaryType())),
    ("upper_bounds", _metric_map_type(BinaryType())),
]

ENTRY_COLUMNS = [
    ("status", IntegerType()),
    ("snapshot_id", LongType()),
    ("sequence_number", LongType()),
]

OUTPUT_COLUMNS = (
    [name for name, _ in ENTRY_COLUMNS]
    + [name for name, _ in DATA_FILE_COLUMNS]
    + [
        "key_metadata",
        "partition_json",
        "manifest_path",
        "partition_spec_id",
        "manifest_content",
        "manifest_sequence_number",
        "manifest_added_snapshot_id",
        "partition_match",
    ]
)


@dataclass(frozen=True)
class PartitionEntries:
    rows: list[Row]
    manifest_errors: dict[str, list[str]]


@timed
def collect_partition_entries(table_name: str, manifests: list[ManifestCandidate], limit: int) -> PartitionEntries:
    entries_df: Optional[pyspark.sql.DataFrame] = None
    manifest_errors: dict[str, list[str]] = {}
    for manifest in manifests:
        try:
            manifest_entries_df = _read_manifest_entries(manifest)

        except Exception as e:
            logger.error(f"[{table_name}] Failed to read manifest {manifest.manifest_path}", exc_info=True)
            manifest_errors[manifest.manifest_path] = [str(e)]
            continue

        entries_df = manifest_entries_df if entries_df is None else entries_df.unionByName(manifest_entries_df)

    if entries_df is None:
        return PartitionEntries(rows=[], manifest_errors=manifest_errors)

    return PartitionEntries(rows=entries_df.limit(limit + 1).collect(), manifest_errors=manifest_errors)


def _read_manifest_entries(manifest: ManifestCandidate) -> pyspark.sql.DataFrame:
    manifest_df = open_spark_connect_session().read.format("avro").load(manifest.manifest_path)
    entry_fields = set(manifest_df.schema.fieldNames())
    data_file_fields = set(manifest_df.schema["data_file"].dataType.fieldNames())

    entry_columns = [_optional_column(F.col(name), name in entry_fields, data_type).alias(name) for name, data_type in ENTRY_COLUMNS]
    data_file = F.col("data_file")
    data_file_columns = [
        _optional_column(data_file.getField(name), name in data_file_fields, data_type).alias(name) for name, data_type in DATA_FILE_COLUMNS
    ]
    key_metadata = _optional_column(F.base64(data_file.getField("key_metadata")), "key_metadata" in data_file_fields, StringType())
    partition_json = F.to_json(data_file.getField("partition"), {"ignoreNullFields": "false"})
    partition_match = PARTITION_MATCH_MATCHED if manifest.spec_filter.is_complete else PARTITION_MATCH_UNDECIDED

    return (
        manifest_df.filter(_entry_filter(manifest))
        .select(
            *entry_columns,
            *data_file_columns,
            key_metadata.alias("key_metadata"),
            partition_json.alias("partition_json"),
            F.lit(manifest.manifest_path).alias("manifest_path"),
            F.lit(manifest.spec_filter.spec_id).cast(IntegerType()).alias("partition_spec_id"),
            F.lit(manifest.content).cast(IntegerType()).alias("manifest_content"),
            F.lit(manifest.sequence_number).cast(LongType()).alias("manifest_sequence_number"),
            F.lit(manifest.added_snapshot_id).cast(LongType()).alias("manifest_added_snapshot_id"),
            F.lit(partition_match).alias("partition_match"),
        )
        .select(*OUTPUT_COLUMNS)
    )


def _optional_column(column: Column, is_present: bool, data_type) -> Column:
    return column.cast(data_type) if is_present else F.lit(None).cast(data_type)


def _entry_filter(manifest: ManifestCandidate) -> Column:
    condition = F.col("status") != MANIFEST_ENTRY_STATUS_DELETED
    partition = F.col("data_file").getField("partition")
    for predicate in manifest.spec_filter.predicates:
        condition = condition & (partition.getField(predicate.spec_field_name) == F.lit(predicate.filter_field.value))

    return condition
