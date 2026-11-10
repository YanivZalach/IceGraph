from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

METADATA_SNAPSHOTS_LINEAGE_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("parent-snapshot-id", LongType()),
            StructField("timestamp-ms", LongType()),
        ]
    )
)
METADATA_REFS_SCHEMA = MapType(StringType(), StructType([StructField("snapshot-id", LongType()), StructField("type", StringType())]))
METADATA_LOG_SCHEMA = ArrayType(StructType([StructField("timestamp-ms", LongType()), StructField("metadata-file", StringType())]))
METADATA_LOG_ENTRIES_SCHEMA = StructType(
    [
        StructField("file", StringType()),
        StructField("timestamp_ms", LongType()),
        StructField("snapshot_id", LongType()),
    ]
)
BRANCH_REF_TYPE = "branch"


@dataclass
class SnapshotCommit:
    snapshot_id: int
    parent_id: Optional[int]
    timestamp_ms: int


@dataclass
class SnapshotRange:
    snapshot_id: int
    start_ms: int
    end_ms: Optional[int]

    def covers(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms and (self.end_ms is None or timestamp_ms < self.end_ms)


class SnapshotRangesBuilder:
    def __init__(self, snapshots: List[SnapshotCommit], live_tip_ids: Set[int]):
        self._snapshots = snapshots
        self._live_tip_ids = live_tip_ids
        self._superseded_at_by_snapshot_id = self._find_superseded_at_by_snapshot_id(snapshots)
        self._sorted_commit_times = sorted(snapshot.timestamp_ms for snapshot in snapshots)

    def build(self) -> List[SnapshotRange]:
        return [SnapshotRange(snapshot.snapshot_id, snapshot.timestamp_ms, self._range_end_ms(snapshot)) for snapshot in self._snapshots]

    def _range_end_ms(self, snapshot: SnapshotCommit) -> Optional[int]:
        superseded_at = self._superseded_at_by_snapshot_id.get(snapshot.snapshot_id)
        if superseded_at is not None:
            return superseded_at

        if snapshot.snapshot_id in self._live_tip_ids:
            return None

        return self._next_commit_time_after(snapshot.timestamp_ms)

    def _next_commit_time_after(self, timestamp_ms: int) -> Optional[int]:
        for commit_time in self._sorted_commit_times:
            if commit_time > timestamp_ms:
                return commit_time

        return None

    @staticmethod
    def _find_superseded_at_by_snapshot_id(snapshots: List[SnapshotCommit]) -> Dict[int, int]:
        superseded_at_by_snapshot_id: Dict[int, int] = {}
        for snapshot in snapshots:
            last_child_commit_time = superseded_at_by_snapshot_id.get(snapshot.parent_id, 0)
            superseded_at_by_snapshot_id[snapshot.parent_id] = max(last_child_commit_time, snapshot.timestamp_ms)

        return superseded_at_by_snapshot_id
