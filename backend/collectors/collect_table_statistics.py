from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from base_classes.base_file import BaseFile, HiddenFile
from base_classes.utils import timed
from collectors.collect_metadata import MetadataFileRecord
from collectors.collector import Collector, FilesCollection
from collectors.statistics_utils import read_table_statistics
from constants import TABLE_STATISTICS_COLLECTION_WARNING, FileType
from icegraph_logger import logger


@dataclass
class HiddenTableStatisticsMetadata(HiddenFile):
    added_by_metadata_file: str


@dataclass
class TableStatisticsFileRecord(BaseFile):
    snapshot_id: int
    file_size_in_bytes: str
    file_footer_size_in_bytes: str
    key_metadata: Optional[str]
    blobs: List[Dict[str, Any]]
    hidden_table_statistics_data: HiddenTableStatisticsMetadata


class CollectTableStatistics(Collector):
    def __init__(
        self,
        full_table_name: str,
        metadata_files: List[MetadataFileRecord],
        statistics_paths_to_ignore: Set[str],
    ):
        super().__init__(full_table_name)
        self._metadata_files = metadata_files
        self._statistics_paths_to_ignore = statistics_paths_to_ignore

        self._table_statistics_files: List[TableStatisticsFileRecord] = []
        self._warnings: Dict[str, List[str]] = {}

    @timed
    def collect(self) -> FilesCollection:
        added_paths_by_metadata_file = self._find_added_statistics_paths()
        if not added_paths_by_metadata_file:
            return FilesCollection()

        try:
            rows = read_table_statistics(self._spark, list(added_paths_by_metadata_file)).collect()
        except Exception as e:
            logger.error(f"[{self._table_name}] Table statistics read error", exc_info=True)
            self._warnings["table_statistics_collection"] = [TABLE_STATISTICS_COLLECTION_WARNING.format(error=e)]
            return FilesCollection(warnings=self._warnings)

        statistics_by_metadata_file = {row.file: row.asDict(recursive=True)["statistics"] or [] for row in rows}

        for metadata_file in self._metadata_files:
            added_paths = added_paths_by_metadata_file.get(metadata_file.file_path)
            if not added_paths:
                continue

            entry_by_path = {}
            for entry in statistics_by_metadata_file.get(metadata_file.file_path, []):
                if entry["statistics-path"] in added_paths:
                    entry_by_path.setdefault(entry["statistics-path"], entry)

            for entry in sorted(entry_by_path.values(), key=lambda entry: entry["statistics-path"]):
                self._add_table_statistics_file(metadata_file, entry)

        return FilesCollection(files=self._table_statistics_files, warnings=self._warnings)

    def _find_added_statistics_paths(self) -> Dict[str, Set[str]]:
        seen_paths = set(self._statistics_paths_to_ignore)
        added_paths_by_metadata_file: Dict[str, Set[str]] = {}

        for metadata_file in reversed(self._metadata_files):
            for statistics_path in (metadata_file.pointed_statistics_files or {}).values():
                if statistics_path in seen_paths:
                    continue

                seen_paths.add(statistics_path)
                added_paths_by_metadata_file.setdefault(metadata_file.file_path, set()).add(statistics_path)

        return added_paths_by_metadata_file

    def _add_table_statistics_file(self, metadata_file: MetadataFileRecord, entry: dict) -> None:
        try:
            table_statistics_file = self._parse_table_statistics_entry(entry, metadata_file.file_path)
        except Exception as e:
            msg = f"Failed to read table statistics entry {entry['statistics-path']}: {type(e).__name__}: {e}"
            logger.error(f"[{self._table_name}] {msg} in {metadata_file.file_path}", exc_info=True)
            metadata_file.warnings.append(msg)
            return

        self._table_statistics_files.append(table_statistics_file)

    @staticmethod
    def _parse_table_statistics_entry(entry: dict, added_by_metadata_file: str) -> TableStatisticsFileRecord:
        return TableStatisticsFileRecord(
            type=FileType.TABLE_STATISTICS,
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
            hidden_table_statistics_data=HiddenTableStatisticsMetadata(added_by_metadata_file=added_by_metadata_file),
        )
