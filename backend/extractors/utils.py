from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

METADATA_SNAPSHOTS_LINEAGE_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("parent-snapshot-id", LongType()),
            StructField("timestamp-ms", LongType()),
        ]
    )
)
METADATA_REFS_SCHEMA = MapType(StringType(), StructType([StructField("snapshot-id", LongType()), StructField("type", StringType())]))
METADATA_LOG_SCHEMA = ArrayType(StructType([StructField("timestamp-ms", LongType()), StructField("metadata-file", StringType())]))
METADATA_LOG_ENTRIES_SCHEMA = StructType(
    [
        StructField("file", StringType()),
        StructField("timestamp_ms", LongType()),
        StructField("snapshot_id", LongType()),
    ]
)
BRANCH_REF_TYPE = "branch"
