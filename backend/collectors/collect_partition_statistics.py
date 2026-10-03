from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from base_classes.base_file import BaseFile
from collectors.collect_metadata import MetadataFileRecord
from collectors.statistics_collector import HiddenStatisticsMetadata, StatisticsCollector
from collectors.utils import format_partition
from constants import FileType
from extractors.partition_statistics_extractor import PartitionStatisticsExtractor
from icegraph_logger import logger

JSON_SAFE_VALUE_TYPES = (bool, int, float, str)


@dataclass
class PartitionStatisticsFileRecord(BaseFile):
    snapshot_id: int
    file_size_in_bytes: str
    partitions_count: Optional[int]
    partition_distribution: Dict[str, Any]
    sampled_partitions: List[Dict[str, Any]]
    hidden_statistics_data: HiddenStatisticsMetadata


class CollectPartitionStatistics(StatisticsCollector):
    STATISTICS_COLUMN = "partition-statistics"
    STATISTICS_KEY = "partition_statistics"
    STATISTICS_NAME = "Partition statistics"

    def _get_pointed_statistics(self, metadata_file: MetadataFileRecord) -> Optional[List[dict]]:
        return metadata_file.pointed_partition_statistics_files

    def _collect_statistics_files(self, metadata_file_to_added_entries: Dict[str, List[dict]]) -> None:
        statistics_files = self._build_statistics_files(metadata_file_to_added_entries)
        self._collect_partition_summaries(statistics_files)

    def _build_statistics_files(self, metadata_file_to_added_entries: Dict[str, List[dict]]) -> Dict[str, PartitionStatisticsFileRecord]:
        statistics_files = {}
        for metadata_file in self._metadata_files:
            for entry in metadata_file_to_added_entries.get(metadata_file.file_path, []):
                statistics_file = PartitionStatisticsFileRecord(
                    type=FileType.PARTITION_STATISTICS,
                    file_path=entry["statistics-path"],
                    child_files=[],
                    snapshot_id=entry["snapshot-id"],
                    file_size_in_bytes=str(entry["file-size-in-bytes"]),
                    partitions_count=None,
                    partition_distribution={},
                    sampled_partitions=[],
                    hidden_statistics_data=HiddenStatisticsMetadata(added_by_metadata_file=metadata_file.file_path),
                )
                statistics_files[statistics_file.file_path] = statistics_file
                self._statistics_files.append(statistics_file)

        return statistics_files

    def _collect_partition_summaries(self, statistics_files: Dict[str, PartitionStatisticsFileRecord]) -> None:
        try:
            rows = PartitionStatisticsExtractor(self._table_name, list(statistics_files.values())).extract_dataframe().collect()
        except Exception as e:
            logger.error(f"[{self._table_name}] Partition statistics batch read error", exc_info=True)
            for statistics_file in statistics_files.values():
                if not statistics_file.errors:
                    statistics_file.errors.append(str(e))
            return

        for row in rows:
            for summary in row.asDict(recursive=True).values():
                statistics_file = statistics_files[summary["file_path"]]
                try:
                    statistics_file.partitions_count = int(summary["partitions_count"])
                    statistics_file.partition_distribution = summary["partition_distribution"] or {}
                    statistics_file.sampled_partitions = [self._parse_partition_row(partition) for partition in summary["sampled_partitions"]]
                except Exception as e:
                    logger.error(f"[{self._table_name}] Partition statistics file parse error for {statistics_file.file_path}", exc_info=True)
                    statistics_file.errors.append(str(e))

    @staticmethod
    def _parse_partition_row(row: dict) -> Dict[str, Any]:
        return {column: CollectPartitionStatistics._parse_partition_value(column, value) for column, value in row.items()}

    @staticmethod
    def _parse_partition_value(column: str, value: Any) -> Any:
        if column == "partition":
            return format_partition(value)

        return value if value is None or isinstance(value, JSON_SAFE_VALUE_TYPES) else str(value)
