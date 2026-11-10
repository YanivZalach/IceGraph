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
class SnapshotRange:
    snapshot_id: int
    start_ms: int
    end_ms: Optional[int]

    def covers(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms and (self.end_ms is None or timestamp_ms < self.end_ms)


def build_snapshot_ranges(snapshots: List[dict], live_tip_ids: Set[int]) -> List[SnapshotRange]:
    superseded_at_by_snapshot_id: Dict[int, int] = {}
    for snapshot in snapshots:
        parent_id = snapshot["parent-snapshot-id"]
        superseded_at_by_snapshot_id[parent_id] = max(superseded_at_by_snapshot_id.get(parent_id, 0), snapshot["timestamp-ms"])

    sorted_commit_times = sorted(snapshot["timestamp-ms"] for snapshot in snapshots)

    return [
        SnapshotRange(
            snapshot_id=snapshot["snapshot-id"],
            start_ms=snapshot["timestamp-ms"],
            end_ms=_range_end_ms(snapshot, superseded_at_by_snapshot_id, live_tip_ids, sorted_commit_times),
        )
        for snapshot in snapshots
    ]


def _range_end_ms(
    snapshot: dict, superseded_at_by_snapshot_id: Dict[int, int], live_tip_ids: Set[int], sorted_commit_times: List[int]
) -> Optional[int]:
    superseded_at = superseded_at_by_snapshot_id.get(snapshot["snapshot-id"])
    if superseded_at is not None:
        return superseded_at

    if snapshot["snapshot-id"] in live_tip_ids:
        return None

    for commit_time in sorted_commit_times:
        if commit_time > snapshot["timestamp-ms"]:
            return commit_time

    return None
