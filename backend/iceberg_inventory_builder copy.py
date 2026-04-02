import arrow
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from typing import Optional, List, Dict, Any, Set
from spark_connect import open_spark_connect_session
from icegraph_logger import logger
from utils import (
    format_metadata_file,
    to_spark_timestamp,
    get_metadata_row_df_from_path,
    _update_col_metric,
    format_partition,
    format_schemas_to_full_dict,
)


class IcebergInventoryBuilder:
    def __init__(self, full_table_name: str, date_to_view: Optional[str] = None):
        self._spark = open_spark_connect_session()
        self._table_name = full_table_name
        self._date_to_view = to_spark_timestamp(date_to_view) if date_to_view else None

        self._cached_dfs = []

        self._timestamp_cutoff = None
        self._manifests_to_ignore_df = None

        self._metadata_files = None

    def collect(self):
        self._find_search_cutoff()
        self._collect_metadata_files()
        self._clean_cache()

    def _cache_df(self, df):
        df.cache()
        self._cached_dfs.append(df)

    def _clean_cache(self):
        for df in self._cached_dfs:
            df.unpersist()
        self._cached_dfs = []

    def _find_search_cutoff(self):
        if not self._date_to_view:
            self._set_no_cutoff()
            return

        baseline_snap_row = self._spark.sql(
            f"SELECT snapshot_id, committed_at FROM {self._table_name}.snapshots WHERE committed_at < '{self._date_to_view}' ORDER BY committed_at DESC LIMIT 1"
        ).collect()

        if not baseline_snap_row:
            self._set_no_cutoff()
            return
        baseline_snap_row = baseline_snap_row[0]

        base_id = baseline_snap_row.snapshot_id
        self._timestamp_cutoff = baseline_snap_row.committed_at
        self._manifests_to_ignore_df = self._spark.sql(
            f"SELECT path FROM {self._table_name}.manifests VERSION AS OF {base_id}"
        )
        self._cache_df(self._manifests_to_ignore_df)

    def _set_no_cutoff(self):
        self._timestamp_cutoff = arrow.Arrow.min
        self._manifests_to_ignore_df = self._spark.createDataFrame(
            [], StructType([StructField("path", StringType())])
        )

    def _collect_metadata_files(self):
        metadata_df = (
            self._spark.sql(f"SELECT * FROM {self._table_name}.metadata_log_entries")
            .withColumnRenamed("timestamp", "metadata_timestamp")
            .select("file", "metadata_timestamp")
        )
        if self._timestamp_cutoff:
            metadata_df = metadata_df.filter(
                F.col("metadata_timestamp") >= F.lit(str(self._timestamp_cutoff))
            )
        self._cache_df(metadata_df)

        metadata_files = {
            row.file: row.metadata_timestamp for row in metadata_df.collect()
        }

        metadata_files_df = None
        for file, timestamp in metadata_files.items():
            df = (
                get_metadata_row_df_from_path(file)
                .withColumn("metadata_timestamp", F.lit(timestamp))
                .withColumn("file", F.lit(file))
            )
            if metadata_files_df is None:
                metadata_files_df = df
            else:
                metadata_files_df = metadata_files_df.unionByName(
                    df, allowMissingColumns=True
                )
        if metadata_files_df is None:
            return []

        parsed_metadata_files = []
        metadata_files_rows = sorted(
            metadata_files_df.collect(), key=lambda row: row.metadata_timestamp
        )
        number_of_metadata_files = len(metadata_files_rows)
        for index, metadata_file in enumerate(metadata_files_rows):
            formatted_metadata_file = format_metadata_file(
                metadata_file=metadata_file,
                is_main_metadata_file=(index == number_of_metadata_files - 1),
                index=index,
                number_of_metadata_files=number_of_metadata_files,
            )
            parsed_metadata_files.append(formatted_metadata_file)

        self._metadata_files = parsed_metadata_files

    def _collect_snapshots(self):
        pass
        


if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    builder = IcebergInventoryBuilder("default.test_table")
    builder.collect()
    print(builder._metadata_files)
