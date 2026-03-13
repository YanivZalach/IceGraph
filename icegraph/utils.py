from typing import List
import os
from contextlib import suppress
from datetime import datetime
from typing import Any, Dict

import arrow
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession

from constants import UI_NEWLINE


def verify_iceberg_table(table_name: str) -> bool:
    with suppress(AnalysisException, AttributeError, IndexError):
        spark = SparkSession.builder.getOrCreate()

        df_desc = spark.sql(f"DESCRIBE FORMATTED {table_name}")
        provider_row = df_desc.filter(df_desc.col_name == "Provider").collect()

        if provider_row:
            provider_value = provider_row[0].data_type.lower().strip()
            return provider_value == "iceberg"

    raise AnalysisException(f"Table '{table_name}' is not an Iceberg table.")


def to_spark_timestamp(date_str: str) -> datetime:
    spark = SparkSession.builder.getOrCreate()
    return (
        arrow.get(date_str)
        .replace(tzinfo=os.environ["TIMEZONE"])
        .to(spark.conf.get("spark.sql.session.timeZone"))
        .datetime
    )


def format_node_info(file_info: Dict[str, Any]) -> str:
    formatted_info = file_info["type"].upper()
    formatted_info += "\n" + "\n".join(
        f"{key}: {value}"
        for key, value in file_info.items()
        if key
        not in [
            "type",
            "child_files",
            "existing_child_files",
            "deleted_child_files",
            "columns",
            "hidden_metadata",  # Not showing to the user
        ]
    )

    if "columns" in file_info and file_info["columns"]:
        all_stats_keys = set()
        for col_stats in file_info["columns"].values():
            all_stats_keys.update(col_stats.keys())

        sorted_keys = sorted(list(all_stats_keys))

        header_cols = [_format_cell(k) for k in sorted_keys]
        header = f"\ncolumns: {_format_cell('Column ID')} | " + " | ".join(header_cols)

        separator = "-" * len(header)
        formatted_info += f"{header}{UI_NEWLINE}{separator}"

        for col_name, stats in file_info["columns"].items():
            row_name = _format_cell(col_name)
            row_values = [_format_cell(stats.get(key, "N/A")) for key in sorted_keys]

            row_str = f"{UI_NEWLINE}{row_name} | " + " | ".join(row_values)
            formatted_info += row_str

    if (
        "existing_child_files" in file_info
        and file_info["existing_child_files"] is not None
    ):
        formatted_info += "\nexisting_child_files:" + _format_list_for_ui(
            file_info["existing_child_files"]
        )
    if (
        "deleted_child_files" in file_info
        and file_info["deleted_child_files"] is not None
    ):
        formatted_info += "\ndeleted_child_files:" + _format_list_for_ui(
            file_info["deleted_child_files"]
        )

    if "child_files" in file_info and file_info["child_files"] is not None:
        formatted_info += "\nchild_files:" + _format_list_for_ui(
            file_info["child_files"]
        )

    return formatted_info


def get_json_metadata_from_path(metadata_path: str) -> Dict[str, Any]:
    spark = SparkSession.builder.getOrCreate()

    row = (
        spark.read.option("multiLine", True)
        .json(metadata_path)
        .drop("metadata-log")
        .drop("snapshot-log")
        .drop("snapshots")
        .drop("statistics")
        .first()
    )

    return row.asDict(recursive=True)


def _update_col_metric(source_list, metric_name, column_metrics):
    for row in source_list:
        col_id = row.key
        if col_id not in column_metrics:
            column_metrics[col_id] = {}
        column_metrics[col_id][metric_name] = str(row.value)


def _format_cell(val: Any, width=40) -> str:
    s_val = str(val)
    if len(s_val) > width:
        return s_val[: width - 3] + "..."
    return f"{s_val:<{width}}"


def _format_list_for_ui(list_to_format: List[str]) -> str:
    return UI_NEWLINE.join(list_to_format)
