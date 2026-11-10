from dataclasses import dataclass
from typing import Dict, List, Optional, Set


@dataclass
class SnapshotCommit:
    snapshot_id: int
    parent_id: Optional[int]
    timestamp_ms: int


@dataclass
class SnapshotLogEntry:
    snapshot_id: int
    timestamp_ms: int


@dataclass
class SnapshotRange:
    snapshot_id: int
    start_ms: int
    end_ms: Optional[int]

    def covers(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms and (self.end_ms is None or timestamp_ms < self.end_ms)


class SnapshotLog:
    def __init__(self, entries: List[SnapshotLogEntry]):
        self._entries = list(reversed(entries))
        self.snapshot_ids = {entry.snapshot_id for entry in entries}

    def snapshot_id_at(self, timestamp_ms: int) -> Optional[int]:
        snapshot_id = None
        for entry in self._entries:
            if entry.timestamp_ms <= timestamp_ms:
                snapshot_id = entry.snapshot_id
                break

        return snapshot_id


class SnapshotRangesBuilder:
    def __init__(self, snapshots: List[SnapshotCommit], main_snapshot_ids: Set[int], side_branch_head_ids: Set[int]):
        self._snapshots = snapshots
        self._main_snapshot_ids = main_snapshot_ids
        self._side_branch_head_ids = side_branch_head_ids
        self._superseded_at_by_snapshot_id = self._find_superseded_at_by_snapshot_id(snapshots)
        self._sorted_commit_times = sorted(snapshot.timestamp_ms for snapshot in snapshots)

    def build(self) -> List[SnapshotRange]:
        snapshot_ranges = []
        for snapshot in self._snapshots:
            if snapshot.snapshot_id in self._side_branch_head_ids:
                snapshot_ranges.append(SnapshotRange(snapshot.snapshot_id, snapshot.timestamp_ms, None))
            elif snapshot.snapshot_id not in self._main_snapshot_ids:
                snapshot_ranges.append(SnapshotRange(snapshot.snapshot_id, snapshot.timestamp_ms, self._range_end_ms(snapshot)))

        return snapshot_ranges

    def _range_end_ms(self, snapshot: SnapshotCommit) -> Optional[int]:
        superseded_at = self._superseded_at_by_snapshot_id.get(snapshot.snapshot_id)
        if superseded_at is not None:
            return superseded_at

        return self._next_commit_time_after(snapshot.timestamp_ms)

    def _next_commit_time_after(self, timestamp_ms: int) -> Optional[int]:
        next_commit_time = None
        for commit_time in self._sorted_commit_times:
            if commit_time > timestamp_ms:
                next_commit_time = commit_time
                break

        return next_commit_time

    @staticmethod
    def _find_superseded_at_by_snapshot_id(snapshots: List[SnapshotCommit]) -> Dict[int, int]:
        superseded_at_by_snapshot_id: Dict[int, int] = {}
        for snapshot in snapshots:
            last_child_commit_time = superseded_at_by_snapshot_id.get(snapshot.parent_id, 0)
            superseded_at_by_snapshot_id[snapshot.parent_id] = max(last_child_commit_time, snapshot.timestamp_ms)

        return superseded_at_by_snapshot_id
