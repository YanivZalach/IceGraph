import arrow
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession


def verify_iceberg_table(table_name: str) -> bool:
    """
    Checks if the given table is using the Iceberg provider.
    """
    try:
        spark = SparkSession.builder.getOrCreate()

        df_desc = spark.sql(f"DESCRIBE FORMATTED {table_name}")
        provider_row = df_desc.filter(df_desc.col_name == "Provider").collect()

        if provider_row:
            provider_value = provider_row[0].data_type.lower().strip()
            return provider_value == "iceberg"
    except:
        pass

    raise AnalysisException("Not an iceberg table")


def to_utc_timestamp(date_str: str):
    return arrow.get(date_str).replace(tzinfo="Asia/Jerusalem").to("UTC").datetime


def format_node_info(file_info: dict):
    formatted_info = file_info["type"].upper() + "\n"
    formatted_info += "\n".join(
        f"{key}: {value}"
        for key, value in file_info.items()
        if key not in ["type", "child_files"]
    )
    if "child_files" in file_info.keys():
        print("inhere")
        formatted_info += "\nchild_files:" + ",".join(file_info["child_files"])

    return formatted_info
