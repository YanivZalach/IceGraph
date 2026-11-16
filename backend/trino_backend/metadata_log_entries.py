import json
from typing import Optional

from extractors.metadata_log_entries_extractor import MetadataLogEntriesExtractor
from icegraph_logger import logger
from trino_backend.metadata_json import json_text, normalize_entries, read_metadata_json
from trino_backend.trino_client import Row, metadata_table, run_query, sql_identifier

METADATA_LOG_FIELDS = ["timestamp-ms", "metadata-file"]
SNAPSHOT_FIELDS = ["snapshot-id", "parent-snapshot-id", "timestamp-ms"]
SNAPSHOT_LOG_FIELDS = ["timestamp-ms", "snapshot-id"]
CURRENT_METADATA_KEYS = ["last-updated-ms", "metadata-log", "snapshots", "snapshot-log", "refs"]


class TrinoMetadataLogEntriesExtractor(MetadataLogEntriesExtractor):
    def _find_current_metadata_entry(self) -> Optional[Row]:
        rows = run_query(f"""
            SELECT file, CAST(round(to_unixtime(timestamp) * 1000) AS bigint) AS timestamp_ms, latest_snapshot_id AS snapshot_id
            FROM {metadata_table(self._table_name, "metadata_log_entries")}
            ORDER BY timestamp DESC, length(file) DESC, file DESC
            LIMIT 1
        """)

        return rows[0] if rows else None

    def _read_current_metadata_row(self, current_metadata_file: str) -> Optional[Row]:
        try:
            metadata_read = read_metadata_json(
                [current_metadata_file], [f"{json_text(key)} AS {sql_identifier(key)}" for key in CURRENT_METADATA_KEYS]
            )
            if current_metadata_file in metadata_read.errors:
                raise ValueError(f"The current metadata file could not be parsed: {metadata_read.errors[current_metadata_file]}")

            metadata = {key: json.loads(metadata_read.rows[current_metadata_file][key] or "null") for key in CURRENT_METADATA_KEYS}

            return Row(
                is_parsed=True,
                file=current_metadata_file,
                last_updated_ms=metadata["last-updated-ms"],
                metadata_log=normalize_entries(metadata["metadata-log"], METADATA_LOG_FIELDS),
                snapshots=normalize_entries(metadata["snapshots"], SNAPSHOT_FIELDS),
                snapshot_log=normalize_entries(metadata["snapshot-log"], SNAPSHOT_LOG_FIELDS),
                refs=metadata["refs"],
            )

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the snapshots from the current metadata file", exc_info=True)
            self.missing_snapshot_reason = str(e)

            return None
