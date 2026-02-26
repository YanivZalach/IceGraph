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


def format_node_info(file_type: str, file_info: dict):
    return (
            file_type.upper() + "\n" + "\n".join(f"{k}: {v}" for k, v in file_info.items())
    )
