from dataclasses import dataclass
from typing import Dict, List, Optional

from base_classes.base_file import BaseFile, HiddenFile
from collectors.collect_manifests import ManifestRecord
from collectors.collector import Collector, FilesCollection
from constants import DATA_FILES_CUTOFF_UNKNOWN_WARNING, DATA_FILES_CUTOFF_WARNING, FileType
from env import Env
from extractors.data_files_extractor import DataFilesExtractor
from collectors.utils import format_partition
from base_classes.utils import timed
from iceberg_ports.readable_metrics import RawFileMetrics


@dataclass
class HiddenDataFileMetadata(HiddenFile):
    pointing_manifests: list[Dict[str, str]]
    raw_metrics: RawFileMetrics


@dataclass
class DataFileRecord(BaseFile):
    format: str
    file_size_in_bytes: str
    row_count: int
    partition: str
    earliest_appearing_snapshot_id: int
    earliest_appearing_snapshot_timestamp: Optional[str]
    sort_order_id: int
    split_offsets: List[int]
    key_metadata: str
    equality_ids: str
    readable_metrics: dict
    hidden_data_file_metadata: HiddenDataFileMetadata


@dataclass
class DataFilesCutoff:
    snapshot_id: int
    snapshot_timestamp: Optional[str]


class CollectDataFiles(Collector):
    def __init__(
        self,
        full_table_name: str,
        manifests: List[ManifestRecord],
    ):
        super().__init__(full_table_name)
        self._manifests = manifests

        self._data_files: List[DataFileRecord] = []

    @timed
    def collect(self) -> FilesCollection:
        data_files_rows = DataFilesExtractor(self._table_name, self._manifests).extract_dataframe().collect()

        self._data_files = [self._process_data_file_row(data_file_row) for data_file_row in data_files_rows if data_file_row.included]

        cutoff = self._find_cutoff(data_files_rows)
        if cutoff is None:
            return FilesCollection(files=self._data_files)

        warnings = {"data_files_cutoff": self._build_cutoff_warning(cutoff)}
        return FilesCollection(files=self._data_files, warnings=warnings, data_files_cutoff_reached=True)

    def _process_data_file_row(self, data_file_row) -> DataFileRecord:
        data_file_dict = data_file_row.asDict(recursive=True)

        return DataFileRecord(
            type=self._detect_file_type(data_file_dict["content"]),
            file_path=data_file_dict["file_path"],
            format=data_file_dict["file_format"],
            file_size_in_bytes=str(data_file_dict["file_size_in_bytes"]),
            row_count=data_file_dict["record_count"],
            partition=format_partition(data_file_dict["partition"]),
            earliest_appearing_snapshot_id=data_file_dict["earliest_snapshot_id"],
            earliest_appearing_snapshot_timestamp=data_file_dict["earliest_snapshot_timestamp"],
            sort_order_id=data_file_dict["sort_order_id"],
            split_offsets=data_file_dict["split_offsets"] or [],
            key_metadata=data_file_dict["key_metadata"],
            equality_ids=data_file_dict["equality_ids"],
            readable_metrics={},
            child_files=[],
            hidden_data_file_metadata=HiddenDataFileMetadata(
                pointing_manifests=data_file_dict["pointing_manifests"],
                raw_metrics=RawFileMetrics(
                    column_sizes=data_file_dict["column_sizes"],
                    value_counts=data_file_dict["value_counts"],
                    null_value_counts=data_file_dict["null_value_counts"],
                    nan_value_counts=data_file_dict["nan_value_counts"],
                    lower_bounds=data_file_dict["lower_bounds"],
                    upper_bounds=data_file_dict["upper_bounds"],
                ),
            ),
        )

    @staticmethod
    def _detect_file_type(content: int) -> FileType:
        if content == 0:
            return FileType.DATA

        if content == 1:
            return FileType.POSITION_DELETE

        return FileType.EQUALITY_DELETE

    @staticmethod
    def _find_cutoff(data_files_rows) -> Optional[DataFilesCutoff]:
        if not data_files_rows:
            return None

        first_row = data_files_rows[0]
        if not first_row.data_files_cutoff_reached:
            return None

        return DataFilesCutoff(first_row.cutoff_snapshot_id, first_row.cutoff_snapshot_timestamp)

    @staticmethod
    def _build_cutoff_warning(cutoff: DataFilesCutoff) -> str:
        if cutoff.snapshot_timestamp is None:
            return DATA_FILES_CUTOFF_UNKNOWN_WARNING.format(max_data_files_to_collect=Env.MAX_DATA_FILES_TO_COLLECT)

        return DATA_FILES_CUTOFF_WARNING.format(
            max_data_files_to_collect=Env.MAX_DATA_FILES_TO_COLLECT,
            added_snapshot_id=cutoff.snapshot_id,
            added_snapshot_timestamp=cutoff.snapshot_timestamp,
        )
