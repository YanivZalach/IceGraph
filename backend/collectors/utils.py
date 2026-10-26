from contextlib import suppress

import arrow
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

from spark_connect import open_spark_connect_session

METADATA_SNAPSHOTS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("manifest-list", StringType()),
        ]
    )
)


def format_partition(partition_dict: dict) -> list[dict]:
    partition = []
    for key, value in (partition_dict or {}).items():
        if value is not None and not isinstance(value, (bool, int, str)):
            value = str(value)

        formatted_value = "null" if value is None else str(value)
        if key.endswith("_hour") and isinstance(value, int) and not isinstance(value, bool):
            with suppress(Exception):
                formatted_value = arrow.Arrow.utcfromtimestamp(value * 3600).format("YYYY-MM-DD HH:00 UTC")

        partition.append({"name": key, "value": value, "formatted_value": formatted_value})

    return partition


def normalize_legacy_v1_metadata_dict(metadata: dict) -> None:
    if "schema" in metadata and "schemas" not in metadata:
        metadata["schemas"] = [{"schema-id": 0, **metadata["schema"]}]
        metadata.setdefault("current-schema-id", metadata["schemas"][0]["schema-id"])

    if "partition-spec" in metadata and "partition-specs" not in metadata:
        metadata["partition-specs"] = [{"spec-id": 0, "fields": metadata["partition-spec"]}]
        metadata.setdefault("default-spec-id", 0)


def get_metadata_row_slim_df_from_path(metadata_path: str):
    spark = open_spark_connect_session()
    df = (
        spark.read.option("ignoreCorruptFiles", True)
        .option("ignoreMissingFiles", True)
        .text(metadata_path, wholetext=True)
        .select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))
    )
    metadata = F.col("metadata")
    has_legacy_schema = F.map_contains_key(metadata, "schema") & ~F.map_contains_key(metadata, "schemas")
    has_legacy_partition_spec = F.map_contains_key(metadata, "partition-spec") & ~F.map_contains_key(metadata, "partition-specs")
    current_schema_id = F.when(
        ~F.map_contains_key(metadata, "current-schema-id") & has_legacy_schema,
        F.coalesce(F.get_json_object(metadata["schema"], "$['schema-id']"), F.lit("0")),
    ).otherwise(metadata["current-schema-id"])
    default_spec_id = F.when(~F.map_contains_key(metadata, "default-spec-id") & has_legacy_partition_spec, F.lit("0")).otherwise(
        metadata["default-spec-id"]
    )
    integer_values = {"current-schema-id": current_schema_id, "default-spec-id": default_spec_id}
    snapshots = F.from_json(metadata["snapshots"], METADATA_SNAPSHOTS_SCHEMA)
    pointed_snapshots = F.transform(
        snapshots,
        lambda snapshot: F.create_map(
            F.lit("snapshot-id"),
            snapshot["snapshot-id"].cast(StringType()),
            F.lit("manifest-list"),
            snapshot["manifest-list"],
        ),
    )
    metadata_log_count = F.when(
        F.map_contains_key(metadata, "metadata-log"), F.size(F.from_json(metadata["metadata-log"], ArrayType(StringType())))
    ).otherwise(F.lit(0))

    integer_cols = [
        "current-schema-id",
        "current-snapshot-id",
        "default-sort-order-id",
        "default-spec-id",
        "format-version",
        "last-column-id",
        "last-partition-id",
        "last-sequence-number",
        "last-updated-ms",
    ]
    string_cols = ["location", "table-uuid"]
    json_cols = ["properties", "refs", "statistics", "partition-statistics"]

    return df.select(
        *[integer_values.get(column, metadata[column]).cast(LongType()).alias(column) for column in integer_cols],
        *[metadata[column].alias(column) for column in string_cols + json_cols],
        metadata_log_count.alias("pointed_metadata_log_count"),
        F.when(F.size(snapshots) > 0, F.to_json(pointed_snapshots)).alias("pointed_snapshots_files"),
    )
