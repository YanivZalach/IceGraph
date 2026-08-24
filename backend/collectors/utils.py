from contextlib import suppress

import arrow
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from spark_connect import open_spark_connect_session


def format_partition(partition_dict: dict) -> str:
    if not partition_dict:
        return "Root"

    partitions_repr = []
    for key, value in partition_dict.items():
        if key.endswith("_hour") and isinstance(value, int):
            with suppress(Exception):
                repr_value = arrow.Arrow.utcfromtimestamp(value * 3600).format("YYYY-MM-DD HH")
                value = f"{repr_value} ({value})"

        partitions_repr.append(f"{key} = {value}")

    return ", ".join(partitions_repr)


def get_metadata_row_slim_df_from_path(metadata_path: str):
    spark = open_spark_connect_session()
    df = spark.read.option("multiLine", True).json(metadata_path)
    df = df.withColumn("pointed_metadata_log_count", F.size(F.col("metadata-log")))

    schema = df.schema
    existing = set(schema.fieldNames())

    if schema["snapshots"].dataType != ArrayType(StringType()):
        existing.add("pointed_snapshots_files")
        df = df.withColumn(
            "pointed_snapshots_files",
            F.transform(
                F.col("snapshots"),
                lambda s: F.create_map(
                    F.lit("snapshot-id"),
                    s.getField("snapshot-id").cast("string"),
                    F.lit("manifest-list"),
                    s.getField("manifest-list"),
                ),
            ),
        )

    scalar_cols = [
        "current-schema-id",
        "current-snapshot-id",
        "default-sort-order-id",
        "default-spec-id",
        "last-column-id",
        "last-partition-id",
        "last-sequence-number",
        "last-updated-ms",
        "location",
        "table-uuid",
        "pointed_metadata_log_count",
    ]
    json_cols = ["properties", "refs", "pointed_snapshots_files"]

    return df.select(
        *[F.col(column) for column in scalar_cols if column in existing],
        *[F.to_json(F.col(column)).alias(column) for column in json_cols if column in existing],
    )
