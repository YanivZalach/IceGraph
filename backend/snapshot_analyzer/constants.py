from dataclasses import dataclass
from typing import FrozenSet, Tuple


@dataclass(frozen=True)
class ReplaceSubOperation:
    label: str
    summary_keys: FrozenSet[str]


REPLACE_SUB_OPERATIONS_BY_PRIORITY: Tuple[ReplaceSubOperation, ...] = (
    ReplaceSubOperation(
        label="rewrite data files",
        summary_keys=frozenset(
            {
                "added-data-files",
                "deleted-data-files",
            }
        ),
    ),
    ReplaceSubOperation(
        label="rewrite delete files",
        summary_keys=frozenset(
            {
                "added-delete-files",
                "removed-delete-files",
                "added-position-delete-files",
                "removed-position-delete-files",
                "added-equality-delete-files",
                "removed-equality-delete-files",
                "added-dvs",
                "removed-dvs",
            }
        ),
    ),
    ReplaceSubOperation(
        label="rewrite manifests",
        summary_keys=frozenset(
            {
                "manifests-created",
                "manifests-replaced",
                "entries-processed",
            }
        ),
    ),
)
