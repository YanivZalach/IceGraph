from functools import cached_property
from typing import List, Optional

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType

from extractors.extractor import Extractor
from extractors.utils import (
    BRANCH_SNAPSHOT_IDS_SCHEMA,
    MAIN_BRANCH,
    METADATA_REFS_SCHEMA,
    METADATA_SNAPSHOTS_LINEAGE_SCHEMA,
    HeadChange,
    ancestry_timeline,
    head_at,
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
        entries = view_df.select("file", F.unix_millis("timestamp").alias("timestamp_ms"), "latest_snapshot_id").collect()
        if not entries:
            return view_df.withColumn("branch_snapshot_ids", F.array().cast(ArrayType(LongType())))

        try:
            ref_timelines = self._read_ref_timelines(max(entries, key=lambda entry: (entry.timestamp_ms, entry.file)).file)

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the branches and tags from the current metadata file", exc_info=True)
            error_lines = str(e).strip().splitlines()
            self.refs_read_error = error_lines[0] if error_lines else type(e).__name__
            ref_timelines = []

        main_snapshot_ids = {entry.latest_snapshot_id for entry in entries}
        branch_rows = [
            (entry.file, sorted({head_at(timeline, entry.timestamp_ms) for timeline in ref_timelines} - main_snapshot_ids - {None}))
            for entry in entries
        ]
        branch_df = self._spark.createDataFrame(branch_rows, BRANCH_SNAPSHOT_IDS_SCHEMA)

        return view_df.join(branch_df, on="file", how="left")

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
