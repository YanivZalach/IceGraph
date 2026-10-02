from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyspark.sql
from pyspark.sql import functions as F

from base_classes.base_file import BaseFile
from collectors.collect_metadata import MetadataFileRecord
from collectors.statistics_collector import HiddenStatisticsMetadata, StatisticsCollector
from collectors.utils import format_partition
from constants import FileType
from env import Env
from icegraph_logger import logger

PARTITION_STATISTICS_FILE_FORMATS = {"parquet", "orc", "avro"}
JSON_SAFE_VALUE_TYPES = (bool, int, float, str)
DEFAULT_PARTITION_STATISTICS_FILE_FORMAT = "parquet"
SAMPLE_ORDER_COLUMN = "last_updated_at"


@dataclass
class PartitionStatisticsFileRecord(BaseFile):
    snapshot_id: int
    file_size_in_bytes: str
    partitions_count: Optional[int]
    sampled_partitions: List[Dict[str, Any]]
    hidden_statistics_data: HiddenStatisticsMetadata


class CollectPartitionStatistics(StatisticsCollector):
    STATISTICS_COLUMN = "partition-statistics"
    STATISTICS_KEY = "partition_statistics"
    STATISTICS_NAME = "Partition statistics"

    def _get_pointed_statistics(self, metadata_file: MetadataFileRecord) -> Optional[List[dict]]:
        return metadata_file.pointed_partition_statistics_files

    def _collect_statistics_files(self, metadata_file_to_added_entries: Dict[str, List[dict]]) -> None:
        for metadata_file in self._metadata_files:
            for entry in metadata_file_to_added_entries.get(metadata_file.file_path, []):
                self._statistics_files.append(self._collect_partition_statistics_file(entry, metadata_file.file_path))

    def _collect_partition_statistics_file(self, entry: dict, added_by_metadata_file: str) -> PartitionStatisticsFileRecord:
        partition_statistics_file = PartitionStatisticsFileRecord(
            type=FileType.PARTITION_STATISTICS,
            file_path=entry["statistics-path"],
            child_files=[],
            snapshot_id=entry["snapshot-id"],
            file_size_in_bytes=str(entry["file-size-in-bytes"]),
            partitions_count=None,
            sampled_partitions=[],
            hidden_statistics_data=HiddenStatisticsMetadata(added_by_metadata_file=added_by_metadata_file),
        )

        try:
            partition_statistics_df = self._read_partition_statistics_file(partition_statistics_file.file_path)
            partition_statistics_file.partitions_count = int(partition_statistics_df.count())
            partition_statistics_file.sampled_partitions = [
                self._parse_partition_row(row.asDict(recursive=True)) for row in self._sample_partitions(partition_statistics_df).collect()
            ]
        except Exception as e:
            logger.error(f"[{self._table_name}] Partition statistics file read error for {partition_statistics_file.file_path}", exc_info=True)
            partition_statistics_file.errors.append(str(e))

        return partition_statistics_file

    def _read_partition_statistics_file(self, file_path: str) -> pyspark.sql.DataFrame:
        file_format = file_path.rsplit(".", 1)[-1].lower()
        if file_format not in PARTITION_STATISTICS_FILE_FORMATS:
            file_format = DEFAULT_PARTITION_STATISTICS_FILE_FORMAT

        return self._spark.read.format(file_format).load(file_path)

    @staticmethod
    def _sample_partitions(partition_statistics_df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        if SAMPLE_ORDER_COLUMN in partition_statistics_df.columns:
            partition_statistics_df = partition_statistics_df.orderBy(F.desc_nulls_last(SAMPLE_ORDER_COLUMN))

        return partition_statistics_df.limit(Env.MAX_PARTITION_STATISTICS_ROWS)

    @staticmethod
    def _parse_partition_row(row: dict) -> Dict[str, Any]:
        return {column: CollectPartitionStatistics._parse_partition_value(column, value) for column, value in row.items()}

    @staticmethod
    def _parse_partition_value(column: str, value: Any) -> Any:
        if column == "partition":
            return format_partition(value)

        return value if value is None or isinstance(value, JSON_SAFE_VALUE_TYPES) else str(value)
