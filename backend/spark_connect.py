import os
import warnings

from pyspark.sql import SparkSession

from icegraph_logger import logger

warnings.filterwarnings(
    "ignore",
    message="WARN WindowExpression: No Partition Defined for Window operation.*",
    category=UserWarning,
    module="pyspark.sql.connect.expressions",
)


def open_spark_connect_session():
    return SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()


def close_spark_connect_session():
    spark = open_spark_connect_session()

    spark.interruptAll()
    spark.stop()

    logger.info("Spark disconnected")
