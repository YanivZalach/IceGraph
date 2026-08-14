from contextlib import suppress
from typing import List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from env import Env


def get_spark_default_catalog(spark: SparkSession) -> str:
    with suppress(Exception):
        return spark.catalog.currentCatalog()

    return "spark_catalog"


def get_spark_catalog_config_value(spark: SparkSession, catalog: str) -> Optional[str]:
    with suppress(Exception):
        return spark.conf.get(f"spark.sql.catalog.{catalog}")
    return None


def is_iceberg_spark_catalog(full_catalog_name: str) -> bool:
    return full_catalog_name == "org.apache.iceberg.spark.SparkCatalog"


def filter_catalogs_to_include(spark: SparkSession, catalogs: list[str]) -> list[str]:
    catalogs_to_include = []
    for catalog in catalogs:
        catalog_config_value = get_spark_catalog_config_value(spark, catalog)

        if Env.INCLUDE_NONE_ICEBERG_CATALOGS or is_iceberg_spark_catalog(catalog_config_value):
            catalogs_to_include.append(catalog)

    return catalogs_to_include


def list_catalog_names(spark: SparkSession) -> list[str]:
    catalogs = spark.sql("SHOW CATALOGS").collect()

    return [row.catalog for row in catalogs]


def collect_databases_in_catalogs(spark: SparkSession, catalogs: List[str]) -> List[str]:
    databases_df = None
    for catalog in catalogs:
        df = spark.sql(f"show databases in {catalog}").withColumn("database", F.concat(F.lit(f"{catalog}."), F.col("namespace"))).select("database")

        if databases_df is None:
            databases_df = df
        else:
            databases_df = databases_df.unionByName(df)

    if databases_df is None:
        return []

    return [row.database for row in databases_df.collect()]


def collect_catalogs_tables_names(spark: SparkSession, databases: List[str]) -> List[str]:
    tables_df = None
    for database in databases:
        df = (
            spark.sql(f"show tables in {database}")
            .withColumn("table", F.concat(F.lit(f"{database}."), F.col("tableName")))
            .select("table", "isTemporary")
        )
        if tables_df is None:
            tables_df = df
        else:
            tables_df = tables_df.unionByName(df)

    if tables_df is None:
        return []

    return [row.table for row in tables_df.filter(F.col("isTemporary") == False).select("table").collect()]
