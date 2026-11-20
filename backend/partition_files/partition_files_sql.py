from datetime import date
from typing import Any

from partition_files.partition_filter import PartitionFilter, SpecPredicate


class PartitionFilesSqlBuilder:
    def __init__(self, table_name: str, snapshot_id: int, partition_filter: PartitionFilter):
        self._table_name = table_name
        self._snapshot_id = snapshot_id
        self._partition_filter = partition_filter

    def build(self) -> str:
        conditions = []
        for filter_field in self._partition_filter.fields:
            condition = f"partition.`{filter_field.name.replace('`', '``')}` = {self._sql_literal(filter_field.value)}"
            lacking_spec_ids = sorted(
                spec_id
                for spec_id, spec_filter in self._partition_filter.spec_filters.items()
                if not self._spec_has_field(spec_filter.predicates, filter_field.field_id)
            )
            if lacking_spec_ids:
                condition = f"({condition} OR spec_id IN ({', '.join(str(spec_id) for spec_id in lacking_spec_ids)}))"
            conditions.append(condition)

        return f"SELECT * FROM {self._table_name}.files VERSION AS OF {self._snapshot_id}\nWHERE " + "\n  AND ".join(conditions)

    @staticmethod
    def _spec_has_field(predicates: list[SpecPredicate], field_id: int) -> bool:
        for predicate in predicates:
            if predicate.filter_field.field_id == field_id:
                return True

        return False

    @staticmethod
    def _sql_literal(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"

        if isinstance(value, date):
            return f"DATE '{value.isoformat()}'"

        if isinstance(value, str):
            escaped_value = value.replace("\\", "\\\\").replace("'", "\\'")

            return f"'{escaped_value}'"

        return str(value)
