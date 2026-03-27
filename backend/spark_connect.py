import os

from pyspark.sql import SparkSession


def open_spark_connect_session():
    return SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).getOrCreate()
