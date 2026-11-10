from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from pyspark.sql import Column, Row, functions as F
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
METADATA_REFS_SCHEMA = MapType(StringType(), StructType([StructField("snapshot-id", LongType())]))
BRANCH_SNAPSHOT_IDS_SCHEMA = StructType([StructField("file", StringType()), StructField("branch_snapshot_ids", ArrayType(LongType()))])
MAIN_BRANCH = "main"


@dataclass
class HeadChange:
    timestamp_ms: int
    snapshot_id: int


def points_at_snapshot(snapshot_id: int) -> Column:
    return (F.col("latest_snapshot_id") == snapshot_id) | F.array_contains("branch_snapshot_ids", snapshot_id)


def ancestry_timeline(head_snapshot_id: Optional[int], snapshots_by_id: Dict[int, Row]) -> List[HeadChange]:
    timeline = []
    visited = set()
    snapshot = snapshots_by_id.get(head_snapshot_id)
    while snapshot is not None and snapshot["snapshot-id"] not in visited:
        visited.add(snapshot["snapshot-id"])
        timeline.append(HeadChange(snapshot["timestamp-ms"], snapshot["snapshot-id"]))
        snapshot = snapshots_by_id.get(snapshot["parent-snapshot-id"])

    return list(reversed(timeline))


def heads_at(ref_timelines: List[List[HeadChange]], timestamp_ms: int) -> Set[int]:
    heads = {head_at(timeline, timestamp_ms) for timeline in ref_timelines}

    return heads - {None}


def head_at(timeline: List[HeadChange], timestamp_ms: int) -> Optional[int]:
    head = None
    for change in timeline:
        if change.timestamp_ms <= timestamp_ms:
            head = change.snapshot_id

    return head
