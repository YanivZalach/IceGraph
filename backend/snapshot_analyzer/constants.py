from dataclasses import dataclass
from typing import Tuple

from constants import REPLACE_OPERATION


@dataclass(frozen=True)
class ReplaceSubOperation:
    label: str
    summary_keys: Tuple[str, ...]

    @property
    def operation(self) -> str:
        return f"{self.label} ({REPLACE_OPERATION})"


REPLACE_SUB_OPERATIONS_BY_PRIORITY: Tuple[ReplaceSubOperation, ...] = (
    ReplaceSubOperation(
        label="rewrite data files",
        summary_keys=(
            "added-data-files",
            "deleted-data-files",
        ),
    ),
    ReplaceSubOperation(
        label="rewrite delete files",
        summary_keys=(
            "added-delete-files",
            "removed-delete-files",
            "added-position-delete-files",
            "removed-position-delete-files",
            "added-equality-delete-files",
            "removed-equality-delete-files",
            "added-dvs",
            "removed-dvs",
        ),
    ),
    ReplaceSubOperation(
        label="rewrite manifests",
        summary_keys=(
            "manifests-created",
            "manifests-replaced",
            "entries-processed",
        ),
    ),
)
