import os

from pyspark.sql import SparkSession

from constants import SPARK_CONNECT


def open_spark_connect_session():
    os.environ["SPARK_REMOTE"] = SPARK_CONNECT
    return SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
