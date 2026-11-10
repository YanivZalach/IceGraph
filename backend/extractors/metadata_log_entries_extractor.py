from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Optional

from pyspark.sql import Column, DataFrame, Row, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

from extractors.extractor import Extractor
from icegraph_logger import logger

SNAPSHOTS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("parent-snapshot-id", LongType()),
            StructField("timestamp-ms", LongType()),
        ]
    )
)
REFS_SCHEMA = MapType(StringType(), StructType([StructField("snapshot-id", LongType())]))
BRANCH_SNAPSHOT_IDS_SCHEMA = StructType([StructField("file", StringType()), StructField("branch_snapshot_ids", ArrayType(LongType()))])
MAIN_BRANCH = "main"


@dataclass
class HeadChange:
    timestamp_ms: int
    snapshot_id: int


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
            (entry.file, sorted({_head_at(timeline, entry.timestamp_ms) for timeline in ref_timelines} - main_snapshot_ids - {None}))
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
                F.from_json(metadata.getItem("snapshots"), SNAPSHOTS_SCHEMA).alias("snapshots"),
                F.from_json(metadata.getItem("refs"), REFS_SCHEMA).alias("refs"),
            )
            .first()
        )
        if row is None or not row.is_parsed:
            raise ValueError(f"The current metadata file could not be parsed: {current_file}")

        snapshots_by_id = {snapshot["snapshot-id"]: snapshot for snapshot in row.snapshots or []}

        return [_ancestry_timeline(ref["snapshot-id"], snapshots_by_id) for ref_name, ref in (row.refs or {}).items() if ref_name != MAIN_BRANCH]


def points_at_snapshot(snapshot_id: int) -> Column:
    return (F.col("latest_snapshot_id") == snapshot_id) | F.array_contains("branch_snapshot_ids", snapshot_id)


def _ancestry_timeline(head_snapshot_id: Optional[int], snapshots_by_id: Dict[int, Row]) -> List[HeadChange]:
    timeline = []
    visited = set()
    snapshot = snapshots_by_id.get(head_snapshot_id)
    while snapshot is not None and snapshot["snapshot-id"] not in visited:
        visited.add(snapshot["snapshot-id"])
        timeline.append(HeadChange(snapshot["timestamp-ms"], snapshot["snapshot-id"]))
        snapshot = snapshots_by_id.get(snapshot["parent-snapshot-id"])

    return list(reversed(timeline))


def _head_at(timeline: List[HeadChange], timestamp_ms: int) -> Optional[int]:
    head = None
    for change in timeline:
        if change.timestamp_ms <= timestamp_ms:
            head = change.snapshot_id

    return head
