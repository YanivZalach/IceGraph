from pyspark.sql import Column, functions as F
from pyspark.sql.types import ArrayType, DataType, LongType, StringType, StructType, TimestampNTZType, TimestampType

from spark_connect import open_spark_connect_session


def partition_with_timestamp_text(partition: Column, partition_type: DataType) -> Column:
    if not isinstance(partition_type, StructType):
        return partition

    for field in partition_type.fields:
        if isinstance(field.dataType, (TimestampType, TimestampNTZType)):
            timestamp = F.to_json(
                F.struct(partition.getField(field.name).alias("value")),
                {
                    "timeZone": "UTC",
                    "timestampFormat": "yyyy-MM-dd HH:mm:ss.SSSSSS",
                    "timestampNTZFormat": "yyyy-MM-dd HH:mm:ss.SSSSSS",
                },
            )
            field_name = "`" + field.name.replace("`", "``") + "`"
            partition = partition.withField(field_name, F.get_json_object(timestamp, "$.value"))

    return partition


def read_metadata_df(metadata_path: str):
    spark = open_spark_connect_session()
    df = spark.read.option("multiLine", True).json(metadata_path)
    df = normalize_legacy_v1_metadata_fields(df)

    return df


def normalize_legacy_v1_metadata_fields(df):
    existing_fields = set(df.columns)

    if "schema" in existing_fields and "schemas" not in existing_fields:
        schema = F.col("schema")

        if "schema-id" not in df.schema["schema"].dataType.fieldNames():
            schema = schema.withField("`schema-id`", F.lit(0))
        df = df.withColumn("schemas", F.array(schema))

        if "current-schema-id" not in existing_fields:
            df = df.withColumn("current-schema-id", schema["schema-id"])

    if "partition-spec" in existing_fields and "partition-specs" not in existing_fields:
        df = df.withColumn("partition-specs", F.array(F.struct(F.lit(0).alias("spec-id"), F.col("partition-spec").alias("fields"))))

        if "default-spec-id" not in existing_fields:
            df = df.withColumn("default-spec-id", F.lit(0))

    return df


def normalize_legacy_v1_metadata_dict(metadata: dict) -> None:
    if "schema" in metadata and "schemas" not in metadata:
        metadata["schemas"] = [{"schema-id": 0, **metadata["schema"]}]
        metadata.setdefault("current-schema-id", metadata["schemas"][0]["schema-id"])

    if "partition-spec" in metadata and "partition-specs" not in metadata:
        metadata["partition-specs"] = [{"spec-id": 0, "fields": metadata["partition-spec"]}]
        metadata.setdefault("default-spec-id", 0)


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
        "pointed_metadata_log_count",
    ]
    string_cols = ["location", "table-uuid"]
    json_cols = ["properties", "refs", "pointed_snapshots_files", "statistics", "partition-statistics"]

    return df.select(
        *[F.col(column).cast(LongType()).alias(column) for column in integer_cols if column in existing],
        *[F.col(column) for column in string_cols if column in existing],
        *[F.to_json(F.col(column)).alias(column) for column in json_cols if column in existing],
    )
