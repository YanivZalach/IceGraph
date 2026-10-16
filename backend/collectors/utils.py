from contextlib import suppress

import arrow
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructType

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


def read_metadata_df(metadata_path: str):
    spark = open_spark_connect_session()
    df = spark.read.option("multiLine", True).json(metadata_path)
    df = normalize_legacy_v1_metadata_fields(df)

    return df


def normalize_legacy_v1_metadata_fields(df):
    existing = set(df.columns)

    if "schema" in existing and "schemas" not in existing:
        schema = F.col("schema")

        if "schema-id" not in df.schema["schema"].dataType.fieldNames():
            schema = schema.withField("`schema-id`", F.lit(0))
        df = df.withColumn("schemas", F.array(schema))

        if "current-schema-id" not in existing:
            df = df.withColumn("current-schema-id", schema["schema-id"])

    if "partition-spec" in existing and "partition-specs" not in existing:
        df = df.withColumn("partition-specs", F.array(F.struct(F.lit(0).alias("spec-id"), F.col("partition-spec").alias("fields"))))

        if "default-spec-id" not in existing:
            df = df.withColumn("default-spec-id", F.lit(0))

    return df


def get_metadata_row_slim_df_from_path(metadata_path: str):
    df = read_metadata_df(metadata_path)
    df = df.withColumn("pointed_metadata_log_count", F.size(F.col("metadata-log")) if "metadata-log" in df.columns else F.lit(0))

    schema = df.schema
    existing = set(schema.fieldNames())

    snapshots_type = schema["snapshots"].dataType if "snapshots" in existing else None
    if isinstance(snapshots_type, ArrayType) and isinstance(snapshots_type.elementType, StructType):

        def pointed_snapshot(snapshot):
            manifest_list = F.lit(None).cast(StringType())
            if "manifest-list" in snapshots_type.elementType.fieldNames():
                manifest_list = snapshot.getField("manifest-list")

            return F.create_map(
                F.lit("snapshot-id"),
                snapshot.getField("snapshot-id").cast(StringType()),
                F.lit("manifest-list"),
                manifest_list,
            )

        existing.add("pointed_snapshots_files")
        df = df.withColumn("pointed_snapshots_files", F.transform(F.col("snapshots"), pointed_snapshot))

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
    json_cols = ["properties", "refs", "pointed_snapshots_files", "statistics", "partition-statistics"]

    return df.select(
        *[F.col(column) for column in scalar_cols if column in existing],
        *[F.to_json(F.col(column)).alias(column) for column in json_cols if column in existing],
    )
