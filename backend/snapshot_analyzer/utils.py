from contextlib import suppress
from typing import FrozenSet, Optional

from constants import REPLACE_OPERATION, SnapshotSummary
from env import Env
from snapshot_analyzer.constants import REPLACE_SUB_OPERATIONS_BY_PRIORITY


def build_action_link(summary: SnapshotSummary) -> Optional[str]:
    app_id = summary.get("spark.app.id")
    if summary.get("engine-name") == "spark":
        app_id = summary.get("app-id", app_id)

    if not Env.SPARK_HISTORY_SERVER_URL or not app_id:
        return None

    return f"{Env.SPARK_HISTORY_SERVER_URL.rstrip('/')}/history/{app_id}/"


def get_replace_operation_from_summary(summary: SnapshotSummary) -> str:
    for sub_operation in REPLACE_SUB_OPERATIONS_BY_PRIORITY:
        if _has_any_positive_summary_count(summary, sub_operation.summary_keys):
            return sub_operation.label

    return REPLACE_OPERATION


def _has_any_positive_summary_count(summary: SnapshotSummary, summary_keys: FrozenSet[str]) -> bool:
    return any(_parse_summary_count(summary[summary_key]) > 0 for summary_key in summary_keys.intersection(summary))


def _parse_summary_count(summary_value: str | float) -> int:
    with suppress(TypeError, ValueError):
        return int(summary_value)

    return 0
