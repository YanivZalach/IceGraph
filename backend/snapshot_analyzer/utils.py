from contextlib import suppress
from typing import Dict, FrozenSet

from constants import REPLACE_OPERATION
from snapshot_analyzer.constants import REPLACE_SUB_OPERATIONS_BY_PRIORITY


def get_replace_operation_from_summary(summary: Dict[str, str]) -> str:
    for sub_operation in REPLACE_SUB_OPERATIONS_BY_PRIORITY:
        if _has_any_positive_summary_count(summary, sub_operation.summary_keys):
            return sub_operation.operation

    return REPLACE_OPERATION


def _has_any_positive_summary_count(summary: Dict[str, str], summary_keys: FrozenSet[str]) -> bool:
    return any(_parse_summary_count(summary[summary_key]) > 0 for summary_key in summary_keys.intersection(summary))


def _parse_summary_count(summary_value: str) -> int:
    with suppress(TypeError, ValueError):
        return int(summary_value)

    return 0
