import json
from typing import Dict, List, Optional, Set

from collectors.collect_table_statistics import TABLE_STATISTICS_COLLECTION_ERROR, TABLE_STATISTICS_SCHEMA, CollectTableStatistics
from constants import STATISTICS_BEFORE_RANGE_ERROR
from icegraph_logger import logger
from trino_backend.metadata_json import json_text, normalize_entries, read_metadata_json
from trino_backend.trino_client import metadata_table, run_query, sql_identifier, sql_string

STATISTICS_ENTRY_FIELDS = TABLE_STATISTICS_SCHEMA.elementType.fieldNames()
BLOB_METADATA_FIELDS = TABLE_STATISTICS_SCHEMA.elementType["blob-metadata"].dataType.elementType.fieldNames()


class TrinoStatisticsMixin:
    def _find_metadata_file_before_range(self) -> Optional[str]:
        metadata_log = metadata_table(self._table_name, "metadata_log_entries")
        rows = run_query(f"""
            SELECT file
            FROM {metadata_log}
            WHERE timestamp < (SELECT max(timestamp) FROM {metadata_log} WHERE file = {sql_string(self._metadata_files[-1].file_path)})
            ORDER BY timestamp DESC
            LIMIT 1
        """)

        return rows[0].file if rows else None

    def _find_statistics_paths_before_range(self) -> Set[str]:
        metadata_file_before_range = self._find_metadata_file_before_range()
        if metadata_file_before_range is None:
            return set()

        metadata_read = read_metadata_json([metadata_file_before_range], [self._statistics_select()])
        if metadata_file_before_range in metadata_read.errors:
            logger.error(
                f"[{self._table_name}] Metadata file before range read error for {metadata_file_before_range}: "
                f"{metadata_read.errors[metadata_file_before_range]}"
            )
            self._errors[f"{self.STATISTICS_KEY}_before_range"] = [
                STATISTICS_BEFORE_RANGE_ERROR.format(metadata_file=metadata_file_before_range, statistics_name=self.STATISTICS_NAME)
            ]

            return set()

        entries = json.loads(metadata_read.rows[metadata_file_before_range].statistics or "null") or []

        return {entry.get("statistics-path") for entry in entries if entry.get("statistics-path") is not None}

    def _statistics_select(self) -> str:
        return f"{json_text(self.STATISTICS_COLUMN)} AS {sql_identifier('statistics')}"


class TrinoCollectTableStatistics(TrinoStatisticsMixin, CollectTableStatistics):
    def _collect_statistics_entries(self, metadata_file_to_added_paths: Dict[str, Set[str]]) -> Optional[Dict[str, List[dict]]]:
        try:
            metadata_read = read_metadata_json(metadata_file_to_added_paths.keys(), [self._statistics_select()])
            if metadata_read.errors:
                raise ValueError("; ".join(metadata_read.errors.values()))

        except Exception as e:
            logger.error(f"[{self._table_name}] Table statistics read error", exc_info=True)
            self._errors["table_statistics_collection"] = [TABLE_STATISTICS_COLLECTION_ERROR.format(error=e)]

            return None

        metadata_file_to_entries = {}
        for metadata_file, added_paths in metadata_file_to_added_paths.items():
            entries = normalize_entries(json.loads(metadata_read.rows[metadata_file].statistics or "null"), STATISTICS_ENTRY_FIELDS) or []
            metadata_file_to_entries[metadata_file] = [
                {**entry, "blob-metadata": normalize_entries(entry["blob-metadata"], BLOB_METADATA_FIELDS)}
                for entry in entries
                if entry["statistics-path"] in added_paths
            ]

        return metadata_file_to_entries
