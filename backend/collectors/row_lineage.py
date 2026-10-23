from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class RowIdAssignment:
    snapshot_id: int
    snapshot_timestamp: str
    first_row_id: int


@dataclass
class RowIdFields:
    first_row_id: Optional[int]
    first_row_id_by_snapshot: Optional[Dict[str, int]]


def earliest_assignment_per_value(assignments: List[RowIdAssignment]) -> List[RowIdAssignment]:
    earliest_by_value: Dict[int, RowIdAssignment] = {}
    for assignment in sorted(assignments, key=lambda candidate: (candidate.snapshot_timestamp, candidate.snapshot_id)):
        if assignment.first_row_id not in earliest_by_value:
            earliest_by_value[assignment.first_row_id] = assignment

    return list(earliest_by_value.values())


def build_row_id_fields(assignments: List[RowIdAssignment]) -> RowIdFields:
    distinct_assignments = earliest_assignment_per_value(assignments)
    if not distinct_assignments:
        return RowIdFields(first_row_id=None, first_row_id_by_snapshot=None)

    if len(distinct_assignments) == 1:
        return RowIdFields(first_row_id=distinct_assignments[0].first_row_id, first_row_id_by_snapshot=None)

    first_row_id_by_snapshot = {str(assignment.snapshot_id): assignment.first_row_id for assignment in distinct_assignments}

    return RowIdFields(first_row_id=None, first_row_id_by_snapshot=first_row_id_by_snapshot)
