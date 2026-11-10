from functools import cached_property
from typing import List, Optional

from pyspark.sql import DataFrame, Row, Window, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType

from constants import NO_REF_LEADS_TO_SNAPSHOT_REASON
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
        self.missing_snapshot_reason = NO_REF_LEADS_TO_SNAPSHOT_REASON

    def extract_dataframe(self) -> DataFrame:
        return self._metadata_log_entries_df

    @cached_property
    def _metadata_log_entries_df(self) -> DataFrame:
        metadata_log_df = self._spark.sql(f"SELECT timestamp, file, latest_snapshot_id FROM {self._table_name}.metadata_log_entries")
        current_metadata_file = self._find_current_metadata_file(metadata_log_df)
        ref_timelines = self._read_ref_timelines(current_metadata_file)
        head_changes_df = self._to_head_changes_df(ref_timelines)

        ref_heads_df = self._match_files_to_ref_heads(metadata_log_df, head_changes_df)
        branch_heads_df = self._exclude_main_heads(ref_heads_df, metadata_log_df)
        branch_snapshot_ids_df = self._group_branch_snapshot_ids_by_file(branch_heads_df)

        return self._join_branch_snapshot_ids(metadata_log_df, branch_snapshot_ids_df)

    @staticmethod
    def _find_current_metadata_file(metadata_log_df: DataFrame) -> Optional[str]:
        current_file_row = metadata_log_df.orderBy(F.desc("timestamp"), F.desc("file")).select("file").first()

        return current_file_row.file if current_file_row else None

    def _read_ref_timelines(self, metadata_file: Optional[str]) -> List[List[HeadChange]]:
        if metadata_file is None:
            return []

        try:
            metadata_row = self._read_snapshots_and_refs(metadata_file)

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the branches and tags from the current metadata file", exc_info=True)
            self.missing_snapshot_reason = str(e)

            return []

        return self._build_ref_timelines(metadata_row)

    def _read_snapshots_and_refs(self, metadata_file: str) -> Row:
        metadata = F.col("metadata")
        row = (
            self._spark.read.text(metadata_file, wholetext=True)
            .select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))
            .select(
                metadata.isNotNull().alias("is_parsed"),
                F.from_json(metadata.getItem("snapshots"), METADATA_SNAPSHOTS_LINEAGE_SCHEMA).alias("snapshots"),
                F.from_json(metadata.getItem("refs"), METADATA_REFS_SCHEMA).alias("refs"),
            )
            .first()
        )
        if row is None or not row.is_parsed:
            raise ValueError(f"The current metadata file could not be parsed: {metadata_file}")

        return row

    @staticmethod
    def _build_ref_timelines(metadata_row: Row) -> List[List[HeadChange]]:
        snapshots_by_id = {snapshot["snapshot-id"]: snapshot for snapshot in metadata_row.snapshots or []}

        return [
            ancestry_timeline(ref["snapshot-id"], snapshots_by_id) for ref_name, ref in (metadata_row.refs or {}).items() if ref_name != MAIN_BRANCH
        ]

    def _to_head_changes_df(self, ref_timelines: List[List[HeadChange]]) -> DataFrame:
        head_changes = [
            (ref_index, position, change.timestamp_ms, change.snapshot_id)
            for ref_index, timeline in enumerate(ref_timelines)
            for position, change in enumerate(timeline)
        ]

        return self._spark.createDataFrame(head_changes, HEAD_CHANGES_SCHEMA)

    @staticmethod
    def _match_files_to_ref_heads(metadata_log_df: DataFrame, head_changes_df: DataFrame) -> DataFrame:
        latest_change_first = Window.partitionBy("file", "ref_index").orderBy(F.desc("position"))

        return (
            metadata_log_df.select("file", F.unix_millis("timestamp").alias("file_timestamp_ms"))
            .join(head_changes_df, F.col("timestamp_ms") <= F.col("file_timestamp_ms"))
            .withColumn("change_rank", F.row_number().over(latest_change_first))
            .filter(F.col("change_rank") == 1)
            .select("file", "snapshot_id")
        )

    @staticmethod
    def _exclude_main_heads(ref_heads_df: DataFrame, metadata_log_df: DataFrame) -> DataFrame:
        main_snapshot_ids_df = metadata_log_df.select(F.col("latest_snapshot_id").alias("snapshot_id")).distinct()

        return ref_heads_df.join(main_snapshot_ids_df, on="snapshot_id", how="left_anti")

    @staticmethod
    def _group_branch_snapshot_ids_by_file(branch_heads_df: DataFrame) -> DataFrame:
        return branch_heads_df.groupBy("file").agg(F.array_sort(F.collect_set("snapshot_id")).alias("branch_snapshot_ids"))

    @staticmethod
    def _join_branch_snapshot_ids(metadata_log_df: DataFrame, branch_snapshot_ids_df: DataFrame) -> DataFrame:
        return metadata_log_df.join(branch_snapshot_ids_df, on="file", how="left").withColumn(
            "branch_snapshot_ids", F.coalesce("branch_snapshot_ids", F.array().cast(ArrayType(LongType())))
        )
