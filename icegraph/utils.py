import json
import os

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


def to_spark_timestamp(date_str: str):
    spark = SparkSession.builder.getOrCreate()
    return (
        arrow.get(date_str)
        .replace(tzinfo=os.environ["TIMEZONE"])
        .to(spark.conf.get("spark.sql.session.timeZone"))
        .datetime
    )


def format_node_info(file_info: dict):
    formatted_info = file_info["type"].upper()
    formatted_info += "\n" + "\n".join(
        f"{key}: {value}"
        for key, value in file_info.items()
        if key not in ["type", "child_files", "columns", "hidden_metadata"]
    )

    if "columns" in file_info and file_info["columns"]:
        all_stats_keys = set()
        for col_stats in file_info["columns"].values():
            all_stats_keys.update(col_stats.keys())

        sorted_keys = sorted(list(all_stats_keys))

        header_cols = [_format_cell(k) for k in sorted_keys]
        header = f"\ncolumns: {_format_cell('Column Name')} | " + " | ".join(
            header_cols
        )

        separator = "-" * len(header)
        formatted_info += f"{header},{separator}"

        for col_name, stats in file_info["columns"].items():
            row_name = _format_cell(col_name)
            row_values = [_format_cell(stats.get(key, "N/A")) for key in sorted_keys]

            row_str = f",{row_name} | " + " | ".join(row_values)
            formatted_info += row_str

    if "child_files" in file_info.keys() and file_info["child_files"] is not None:
        formatted_info += "\nchild_files:" + ",".join(file_info["child_files"])

    return formatted_info


def get_json_metadata_from_path(metadata_path):
    spark = SparkSession.builder.getOrCreate()

    json_lines = spark.read.text(metadata_path).collect()
    metadata_json = "\n".join([row["value"] for row in json_lines])

    return json.loads(metadata_json)


def _update_col_metric(source_list, metric_name, column_metrics):
    for row in source_list:
        col_id = row.key
        if col_id not in column_metrics:
            column_metrics[col_id] = {}
        column_metrics[col_id][metric_name] = str(row.value)


def _format_cell(val, width=40):
    s_val = str(val)
    if len(s_val) > width:
        return s_val[: width - 3] + "..."
    return f"{s_val:<{width}}"
