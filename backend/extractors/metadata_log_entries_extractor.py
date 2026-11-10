from functools import cached_property
from typing import List, Optional

from pyspark.sql import DataFrame, Row, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType

from constants import NO_REF_LEADS_TO_SNAPSHOT_REASON
from extractors.extractor import Extractor
from extractors.utils import (
    BRANCH_SNAPSHOT_IDS_SCHEMA,
    MAIN_BRANCH,
    METADATA_REFS_SCHEMA,
    METADATA_SNAPSHOTS_LINEAGE_SCHEMA,
    HeadChange,
    ancestry_timeline,
    heads_at,
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
        metadata_log_entries = self._collect_metadata_log_entries(metadata_log_df)
        current_metadata_file = self._find_current_metadata_file(metadata_log_entries)
        ref_timelines = self._read_ref_timelines(current_metadata_file)

        branch_snapshot_ids_df = self._to_branch_snapshot_ids_df(metadata_log_entries, ref_timelines)

        return self._join_branch_snapshot_ids(metadata_log_df, branch_snapshot_ids_df)

    @staticmethod
    def _collect_metadata_log_entries(metadata_log_df: DataFrame) -> List[Row]:
        return metadata_log_df.select("file", F.unix_millis("timestamp").alias("timestamp_ms"), "latest_snapshot_id").collect()

    @staticmethod
    def _find_current_metadata_file(metadata_log_entries: List[Row]) -> Optional[str]:
        if not metadata_log_entries:
            return None

        return max(metadata_log_entries, key=lambda entry: (entry.timestamp_ms, entry.file)).file

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

    def _to_branch_snapshot_ids_df(self, metadata_log_entries: List[Row], ref_timelines: List[List[HeadChange]]) -> DataFrame:
        main_snapshot_ids = {entry.latest_snapshot_id for entry in metadata_log_entries}
        branch_snapshot_ids = [
            (entry.file, sorted(heads_at(ref_timelines, entry.timestamp_ms) - main_snapshot_ids)) for entry in metadata_log_entries
        ]

        return self._spark.createDataFrame(branch_snapshot_ids, BRANCH_SNAPSHOT_IDS_SCHEMA)

    @staticmethod
    def _join_branch_snapshot_ids(metadata_log_df: DataFrame, branch_snapshot_ids_df: DataFrame) -> DataFrame:
        return metadata_log_df.join(branch_snapshot_ids_df, on="file", how="left").withColumn(
            "branch_snapshot_ids", F.coalesce("branch_snapshot_ids", F.array().cast(ArrayType(LongType())))
        )
