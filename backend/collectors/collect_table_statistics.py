from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType

from base_classes.base_file import BaseFile
from collectors.collect_metadata import MetadataFileRecord
from collectors.statistics_collector import HiddenStatisticsMetadata, StatisticsCollector
from constants import TABLE_STATISTICS_COLLECTION_ERROR, FileType
from icegraph_logger import logger

TABLE_STATISTICS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("statistics-path", StringType()),
            StructField("file-size-in-bytes", LongType()),
            StructField("file-footer-size-in-bytes", LongType()),
            StructField("key-metadata", StringType()),
            StructField(
                "blob-metadata",
                ArrayType(
                    StructType(
                        [
                            StructField("type", StringType()),
                            StructField("snapshot-id", LongType()),
                            StructField("sequence-number", LongType()),
                            StructField("fields", ArrayType(IntegerType())),
                            StructField("properties", MapType(StringType(), StringType())),
                        ]
                    )
                ),
            ),
        ]
    )
)

METADATA_FILE_STATISTICS_SCHEMA = StructType([StructField("statistics", TABLE_STATISTICS_SCHEMA)])


@dataclass
class TableStatisticsFileRecord(BaseFile):
    snapshot_id: int
    file_size_in_bytes: str
    file_footer_size_in_bytes: str
    key_metadata: Optional[str]
    blobs: List[Dict[str, Any]]
    hidden_statistics_data: HiddenStatisticsMetadata


class CollectTableStatistics(StatisticsCollector):
    STATISTICS_COLUMN = "statistics"
    STATISTICS_KEY = "table_statistics"
    STATISTICS_NAME = "Table statistics"

    def _get_pointed_statistics(self, metadata_file: MetadataFileRecord) -> Optional[List[dict]]:
        return metadata_file.pointed_table_statistics_files

    def _collect_statistics_files(self, metadata_file_to_added_entries: Dict[str, List[dict]]) -> None:
        metadata_file_to_added_paths = {
            metadata_file: {entry["statistics-path"] for entry in entries} for metadata_file, entries in metadata_file_to_added_entries.items()
        }
        metadata_file_to_statistics_entries = self._collect_statistics_entries(metadata_file_to_added_paths)
        if metadata_file_to_statistics_entries is None:
            return

        for metadata_file in self._metadata_files:
            entries = metadata_file_to_statistics_entries.get(metadata_file.file_path, [])
            for entry in sorted(entries, key=lambda entry: entry["statistics-path"]):
                self._add_table_statistics_file(metadata_file, entry)

    def _collect_statistics_entries(self, metadata_file_to_added_paths: Dict[str, Set[str]]) -> Optional[Dict[str, List[dict]]]:
        try:
            rows = self._read_metadata_statistics(
                list(metadata_file_to_added_paths.keys()),
                METADATA_FILE_STATISTICS_SCHEMA,
                metadata_file_to_added_paths,
            ).collect()
        except Exception as e:
            logger.error(f"[{self._table_name}] Table statistics read error", exc_info=True)
            self._errors["table_statistics_collection"] = [TABLE_STATISTICS_COLLECTION_ERROR.format(error=e)]
            return None

        return {row.file: row.asDict(recursive=True)[self.STATISTICS_COLUMN] or [] for row in rows}

    def _add_table_statistics_file(self, metadata_file: MetadataFileRecord, entry: dict) -> None:
        try:
            table_statistics_file = self._parse_table_statistics_entry(entry, metadata_file.file_path)
        except Exception as e:
            msg = f"Failed to read table statistics entry {entry['statistics-path']}: {type(e).__name__}: {e}"
            logger.error(f"[{self._table_name}] {msg} in {metadata_file.file_path}", exc_info=True)
            metadata_file.errors.append(msg)
            return

        self._statistics_files.append(table_statistics_file)

    @staticmethod
    def _parse_table_statistics_entry(entry: dict, added_by_metadata_file: str) -> TableStatisticsFileRecord:
        return TableStatisticsFileRecord(
            type=FileType.TABLE_STATISTICS,
            virtual_read=True,
            file_path=entry["statistics-path"],
            child_files=[],
            snapshot_id=entry["snapshot-id"],
            file_size_in_bytes=str(entry["file-size-in-bytes"]),
            file_footer_size_in_bytes=str(entry["file-footer-size-in-bytes"]),
            key_metadata=entry["key-metadata"],
            blobs=[
                {
                    "type": blob["type"],
                    "fields": blob["fields"],
                    "snapshot_id": blob["snapshot-id"],
                    "sequence_number": blob["sequence-number"],
                    "properties": blob["properties"] or {},
                }
                for blob in entry["blob-metadata"]
            ],
            hidden_statistics_data=HiddenStatisticsMetadata(added_by_metadata_file=added_by_metadata_file),
        )
