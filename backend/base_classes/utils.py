from constants import STANDART_DATE_FORMAT
from spark_connect import open_spark_connect_session
import functools
import inspect
import time
from contextlib import suppress

import arrow
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from icegraph_logger import logger


def timed(fn):
    signature = inspect.signature(fn)

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        start = time.time()

        bound = signature.bind_partial(*args, **kwargs)
        table_name = bound.arguments.get("table_name")
        if not table_name and (obj := bound.arguments.get("self")):
            table_name = getattr(
                obj,
                "_table_name",
                None,
            ) or getattr(obj, "table_name", None)

        result = fn(*args, **kwargs)

        prefix = f"[{table_name}] " if table_name else ""
        logger.info(f"{prefix}{fn.__qualname__} took {time.time() - start:.2f}s")

        return result

    return wrapper


def verify_iceberg_table(table_name: str) -> bool:
    spark = open_spark_connect_session()

    with suppress(AnalysisException, AttributeError, IndexError):
        provider_row = spark.sql(f"DESCRIBE FORMATTED {table_name}").filter(F.col("col_name") == "Provider").collect()
        if provider_row:
            return provider_row[0].data_type.lower().strip() == "iceberg"

    raise AnalysisException(f"Table '{table_name}' is not an Iceberg table.")


def to_arrow_utc(timestamp):
    return arrow.get(timestamp).replace(tzinfo="UTC")


def column_to_string_utc(column_name: str):
    """
    Converts a timestamp column to a string in UTC format.

    Note: In case of daylight saving time, as the timezone is changed, the timestamp will be converted to UTC and then back to the local time. This can on the hour of the shift cause incorrect results.

    Args:
        column_name: The name of the column to convert.

    Returns:
        pyspark.sql.functions.Column: The converted column.
    """
    session = open_spark_connect_session()
    local_tz = session.conf.get("spark.sql.session.timeZone")

    string_column_with_local_tz = F.date_format(F.col(column_name), STANDART_DATE_FORMAT)
    timestamp_column_at_utc = F.to_utc_timestamp(string_column_with_local_tz, local_tz)

    return F.date_format(timestamp_column_at_utc, STANDART_DATE_FORMAT)


def collect_graph_metadata_file(table_name: str, end_snapshot_id: int | None) -> str:
    spark = open_spark_connect_session()
    metadata_entries = spark.sql(f"SELECT timestamp, file, latest_snapshot_id FROM {table_name}.metadata_log_entries")

    if end_snapshot_id is not None:
        metadata_entries = metadata_entries.filter(F.col("latest_snapshot_id") == end_snapshot_id)

    selected_entry = metadata_entries.orderBy(F.desc("timestamp")).select("file").first()

    if not selected_entry:
        if end_snapshot_id is None:
            raise ValueError(f"No metadata files found for table {table_name}")
        raise ValueError(f"No metadata file found for table {table_name} at snapshot {end_snapshot_id}")

    return selected_entry.file


def format_snapshot_summary(summary: dict[str, str]) -> dict[str, str]:
    formatted = {}
    for key, value in summary.items():
        if key.endswith("files-size"):
            formatted[f"{key}-bytes"] = str(value)
        else:
            formatted[key] = value

    return formatted
