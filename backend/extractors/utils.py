from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from pyspark.sql import Row
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
        StructField("snapshot_ids", ArrayType(LongType())),
    ]
)
BRANCH_REF_TYPE = "branch"


@dataclass
class SnapshotRange:
    snapshot_id: int
    start_ms: int
    end_ms: Optional[int]

    def covers(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms and (self.end_ms is None or timestamp_ms < self.end_ms)


def build_snapshot_ranges(snapshots: List[Row], live_tip_ids: Set[int]) -> List[SnapshotRange]:
    child_timestamps: Dict[int, List[int]] = defaultdict(list)
    for snapshot in snapshots:
        child_timestamps[snapshot["parent-snapshot-id"]].append(snapshot["timestamp-ms"])

    commit_timestamps = sorted(snapshot["timestamp-ms"] for snapshot in snapshots)

    return [
        SnapshotRange(
            snapshot_id=snapshot["snapshot-id"],
            start_ms=snapshot["timestamp-ms"],
            end_ms=_range_end_ms(snapshot, child_timestamps, live_tip_ids, commit_timestamps),
        )
        for snapshot in snapshots
    ]


def _range_end_ms(snapshot: Row, child_timestamps: Dict[int, List[int]], live_tip_ids: Set[int], commit_timestamps: List[int]) -> Optional[int]:
    children = child_timestamps.get(snapshot["snapshot-id"])
    if children:
        return max(children)

    if snapshot["snapshot-id"] in live_tip_ids:
        return None

    for commit_timestamp in commit_timestamps:
        if commit_timestamp > snapshot["timestamp-ms"]:
            return commit_timestamp

    return None
