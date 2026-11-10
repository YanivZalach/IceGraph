from functools import cached_property
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType

from extractors.extractor import Extractor
from extractors.utils import (
    HEAD_CHANGES_SCHEMA,
    MAIN_BRANCH,
    METADATA_REFS_SCHEMA,
    METADATA_SNAPSHOTS_LINEAGE_SCHEMA,
    HeadChange,
    ancestry_timeline,
)
from icegraph_logger import logger


class MetadataLogEntriesExtractor(Extractor):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.refs_read_error: Optional[str] = None

    def extract_dataframe(self) -> DataFrame:
        return self._metadata_log_entries_df

    @cached_property
    def _metadata_log_entries_df(self) -> DataFrame:
        view_df = self._spark.sql(f"SELECT timestamp, file, latest_snapshot_id FROM {self._table_name}.metadata_log_entries")
        branch_heads_df = self._branch_heads_df(view_df)

        return view_df.join(branch_heads_df, on="file", how="left").withColumn(
            "branch_snapshot_ids", F.coalesce("branch_snapshot_ids", F.array().cast(ArrayType(LongType())))
        )

    def _branch_heads_df(self, view_df: DataFrame) -> DataFrame:
        head_changes_df = self._spark.createDataFrame(self._read_head_changes(view_df), HEAD_CHANGES_SCHEMA)
        main_snapshot_ids_df = view_df.select(F.col("latest_snapshot_id").alias("snapshot_id")).distinct()
        latest_change_first = Window.partitionBy("file", "ref_index").orderBy(F.desc("position"))

        return (
            view_df.select("file", F.unix_millis("timestamp").alias("file_timestamp_ms"))
            .join(head_changes_df, F.col("timestamp_ms") <= F.col("file_timestamp_ms"))
            .withColumn("change_rank", F.row_number().over(latest_change_first))
            .filter(F.col("change_rank") == 1)
            .join(main_snapshot_ids_df, on="snapshot_id", how="left_anti")
            .groupBy("file")
            .agg(F.array_sort(F.collect_set("snapshot_id")).alias("branch_snapshot_ids"))
        )

    def _read_head_changes(self, view_df: DataFrame) -> List[Tuple[int, int, int, int]]:
        current_file_row = view_df.orderBy(F.desc("timestamp"), F.desc("file")).select("file").first()
        if current_file_row is None:
            return []

        try:
            ref_timelines = self._read_ref_timelines(current_file_row.file)

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the branches and tags from the current metadata file", exc_info=True)
            error_lines = str(e).strip().splitlines()
            self.refs_read_error = error_lines[0] if error_lines else type(e).__name__

            return []

        return [
            (ref_index, position, change.timestamp_ms, change.snapshot_id)
            for ref_index, timeline in enumerate(ref_timelines)
            for position, change in enumerate(timeline)
        ]

    def _read_ref_timelines(self, current_file: str) -> List[List[HeadChange]]:
        metadata = F.col("metadata")
        row = (
            self._spark.read.text(current_file, wholetext=True)
            .select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))
            .select(
                metadata.isNotNull().alias("is_parsed"),
                F.from_json(metadata.getItem("snapshots"), METADATA_SNAPSHOTS_LINEAGE_SCHEMA).alias("snapshots"),
                F.from_json(metadata.getItem("refs"), METADATA_REFS_SCHEMA).alias("refs"),
            )
            .first()
        )
        if row is None or not row.is_parsed:
            raise ValueError(f"The current metadata file could not be parsed: {current_file}")

        snapshots_by_id = {snapshot["snapshot-id"]: snapshot for snapshot in row.snapshots or []}

        return [ancestry_timeline(ref["snapshot-id"], snapshots_by_id) for ref_name, ref in (row.refs or {}).items() if ref_name != MAIN_BRANCH]
