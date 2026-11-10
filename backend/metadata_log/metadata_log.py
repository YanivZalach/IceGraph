from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Optional

import arrow
from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

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
MAIN_BRANCH = "main"


@dataclass
class MetadataLogEntry:
    file: str
    timestamp_ms: int

    @property
    def timestamp(self) -> arrow.Arrow:
        return arrow.get(self.timestamp_ms / 1000)


@dataclass
class HeadChange:
    timestamp_ms: int
    snapshot_id: int


@dataclass
class RefMetadataWindow:
    first_entry: MetadataLogEntry
    last_entry: MetadataLogEntry


class MetadataLog:
    def __init__(self, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table_name = table_name

    def find_ref_window(self, snapshot_id: int) -> Optional[RefMetadataWindow]:
        pointing_entries = [entry for entry in self._entries if any(_head_at(timeline, entry) == snapshot_id for timeline in self._ref_timelines)]
        if not pointing_entries:
            return None

        return RefMetadataWindow(first_entry=pointing_entries[-1], last_entry=pointing_entries[0])

    @cached_property
    def _entries(self) -> List[MetadataLogEntry]:
        rows = (
            self._spark.sql(f"SELECT file, timestamp FROM {self._table_name}.metadata_log_entries")
            .select("file", F.unix_millis("timestamp").alias("timestamp_ms"))
            .orderBy(F.desc("timestamp_ms"), F.desc("file"))
            .collect()
        )

        return [MetadataLogEntry(file=row.file, timestamp_ms=row.timestamp_ms) for row in rows]

    @cached_property
    def _ref_timelines(self) -> List[List[HeadChange]]:
        if not self._entries:
            return []

        current_file = self._entries[0].file
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


def _ancestry_timeline(head_snapshot_id: Optional[int], snapshots_by_id: Dict[int, Row]) -> List[HeadChange]:
    timeline = []
    visited = set()
    snapshot = snapshots_by_id.get(head_snapshot_id)
    while snapshot is not None and snapshot["snapshot-id"] not in visited:
        visited.add(snapshot["snapshot-id"])
        timeline.append(HeadChange(snapshot["timestamp-ms"], snapshot["snapshot-id"]))
        snapshot = snapshots_by_id.get(snapshot["parent-snapshot-id"])

    return list(reversed(timeline))


def _head_at(timeline: List[HeadChange], entry: MetadataLogEntry) -> Optional[int]:
    head = None
    for change in timeline:
        if change.timestamp_ms <= entry.timestamp_ms:
            head = change.snapshot_id

    return head
