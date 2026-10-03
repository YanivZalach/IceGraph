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
    partitions_with_deletes: Optional[int]
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

    def collect_file(self, metadata_path: str, entry: dict, include_samples: bool = True) -> PartitionStatisticsFileRecord:
        statistics_file = self._parse_statistics_entry(metadata_path, entry)
        self._collect_partition_summaries({statistics_file.file_path: statistics_file}, include_samples)
        return statistics_file

    def _build_statistics_files(self, metadata_file_to_added_entries: Dict[str, List[dict]]) -> Dict[str, PartitionStatisticsFileRecord]:
        statistics_files = {}
        for metadata_file in self._metadata_files:
            for entry in metadata_file_to_added_entries.get(metadata_file.file_path, []):
                statistics_file = self._parse_statistics_entry(metadata_file.file_path, entry)
                statistics_files[statistics_file.file_path] = statistics_file
                self._statistics_files.append(statistics_file)

        return statistics_files

    @staticmethod
    def _parse_statistics_entry(metadata_path: str, entry: dict) -> PartitionStatisticsFileRecord:
        return PartitionStatisticsFileRecord(
            type=FileType.PARTITION_STATISTICS,
            file_path=entry["statistics-path"],
            child_files=[],
            snapshot_id=entry["snapshot-id"],
            file_size_in_bytes=str(entry["file-size-in-bytes"]),
            partitions_count=None,
            partitions_with_deletes=None,
            partition_distribution={},
            sampled_partitions=[],
            hidden_statistics_data=HiddenStatisticsMetadata(added_by_metadata_file=metadata_path),
        )

    def _collect_partition_summaries(self, statistics_files: Dict[str, PartitionStatisticsFileRecord], include_samples: bool = True) -> None:
        try:
            rows = PartitionStatisticsExtractor(self._table_name, list(statistics_files.values())).extract_dataframe(include_samples).collect()
        except Exception as e:
            logger.error(f"[{self._table_name}] Partition statistics batch read error", exc_info=True)
            for statistics_file in statistics_files.values():
                if not statistics_file.errors:
                    statistics_file.errors.append(str(e))
            return

        for row in rows:
            statistics_row = row.asDict(recursive=True)
            statistics_file = statistics_files[statistics_row["file_path"]]
            try:
                statistics_file.partitions_count = int(statistics_row["summary"]["partitions_count"])
                statistics_file.partitions_with_deletes = statistics_row["summary"]["partitions_with_deletes"]
                statistics_file.partition_distribution = {
                    metric: value for metric, value in (statistics_row["summary"]["partition_distribution"] or {}).items() if value is not None
                }
                sampled_partitions = sorted(
                    statistics_row.get("sampled_partitions", []),
                    key=lambda partition: partition["last_updated_at"] if partition.get("last_updated_at") is not None else float("-inf"),
                    reverse=True,
                )
                statistics_file.sampled_partitions = [self._parse_partition_row(partition) for partition in sampled_partitions]
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
