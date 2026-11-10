from dataclasses import dataclass
from functools import cached_property
from typing import Dict, List, Optional, Set

import arrow
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

from base_classes.utils import column_to_string_utc, timed, to_arrow_utc

SNAPSHOT_LOG_SCHEMA = ArrayType(StructType([StructField("timestamp-ms", LongType()), StructField("snapshot-id", LongType())]))
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
    timestamp: arrow.Arrow
    timestamp_ms: int


@dataclass
class HeadChange:
    timestamp_ms: int
    snapshot_id: int


@dataclass
class SnapshotMetadataWindow:
    first_entry: MetadataLogEntry
    last_entry: MetadataLogEntry
    is_open_ended: bool
    is_main_history: bool


@dataclass
class RefTimelines:
    main: List[HeadChange]
    other_refs: List[List[HeadChange]]


class MetadataLog:
    def __init__(self, spark: SparkSession, table_name: str):
        self._spark = spark
        self._table_name = table_name

    @cached_property
    def entries(self) -> List[MetadataLogEntry]:
        rows = (
            self._spark.sql(f"SELECT file, timestamp FROM {self._table_name}.metadata_log_entries")
            .orderBy(F.desc("timestamp"), F.desc("file"))
            .withColumn("timestamp_ms", F.unix_millis("timestamp"))
            .withColumn("timestamp", column_to_string_utc("timestamp"))
            .collect()
        )

        return [MetadataLogEntry(file=row.file, timestamp=to_arrow_utc(row.timestamp), timestamp_ms=row.timestamp_ms) for row in rows]

    def latest_entry_at_or_before(self, cutoff: arrow.Arrow) -> Optional[MetadataLogEntry]:
        for entry in self.entries:
            if entry.timestamp <= cutoff:
                return entry

        return None

    @timed
    def find_snapshot_windows(self, snapshot_ids: Set[int]) -> Dict[int, SnapshotMetadataWindow]:
        if not snapshot_ids or not self.entries:
            return {}

        timelines = self._read_ref_timelines()
        windows = {}
        for snapshot_id in snapshot_ids:
            window = self._window_for(snapshot_id, timelines)
            if window is not None:
                windows[snapshot_id] = window

        return windows

    def _read_ref_timelines(self) -> RefTimelines:
        metadata = F.col("metadata")
        row = (
            self._spark.read.text(self.entries[0].file, wholetext=True)
            .select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))
            .select(
                metadata.isNotNull().alias("is_parsed"),
                F.from_json(metadata.getItem("snapshot-log"), SNAPSHOT_LOG_SCHEMA).alias("snapshot_log"),
                F.from_json(metadata.getItem("snapshots"), SNAPSHOTS_SCHEMA).alias("snapshots"),
                F.from_json(metadata.getItem("refs"), REFS_SCHEMA).alias("refs"),
            )
            .first()
        )
        if row is None or not row.is_parsed:
            raise ValueError(f"The current metadata file could not be parsed: {self.entries[0].file}")

        main = [HeadChange(entry["timestamp-ms"], entry["snapshot-id"]) for entry in row.snapshot_log or []]
        snapshots_by_id = {snapshot["snapshot-id"]: snapshot for snapshot in row.snapshots or []}
        other_refs = []
        for ref_name in sorted(row.refs or {}):
            if ref_name != MAIN_BRANCH:
                other_refs.append(_ancestry_timeline(row.refs[ref_name]["snapshot-id"], snapshots_by_id))

        return RefTimelines(main=main, other_refs=other_refs)

    def _window_for(self, snapshot_id: int, timelines: RefTimelines) -> Optional[SnapshotMetadataWindow]:
        main_entries = [entry for entry in self.entries if _head_at(timelines.main, entry) == snapshot_id]
        pointing_entries = main_entries or [
            entry for entry in self.entries if any(_head_at(timeline, entry) == snapshot_id for timeline in timelines.other_refs)
        ]
        if not pointing_entries:
            return None

        return SnapshotMetadataWindow(
            first_entry=pointing_entries[-1],
            last_entry=pointing_entries[0],
            is_open_ended=pointing_entries[0] is self.entries[0],
            is_main_history=bool(main_entries),
        )


def _ancestry_timeline(head_snapshot_id: Optional[int], snapshots_by_id: Dict[int, dict]) -> List[HeadChange]:
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
        if change.timestamp_ms is not None and change.timestamp_ms <= entry.timestamp_ms:
            head = change.snapshot_id

    return head
