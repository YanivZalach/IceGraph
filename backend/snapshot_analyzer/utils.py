from contextlib import suppress
from typing import Dict, Iterable

from constants import REPLACE_OPERATION
from snapshot_analyzer.constants import REPLACE_SUB_OPERATIONS_BY_PRIORITY


def describe_replace(summary: Dict[str, str]) -> str:
    for sub_operation in REPLACE_SUB_OPERATIONS_BY_PRIORITY:
        if has_positive_count(summary, sub_operation.summary_keys):
            return sub_operation.operation

    return REPLACE_OPERATION


def has_positive_count(summary: Dict[str, str], summary_keys: Iterable[str]) -> bool:
    for summary_key in summary_keys:
        with suppress(TypeError, ValueError):
            if int(summary.get(summary_key, 0)) > 0:
                return True

    return False
