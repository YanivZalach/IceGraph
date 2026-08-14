from dataclasses import dataclass
from typing import Dict, List, Optional

from base_classes.base_file import BaseFile, HiddenFile
from collectors.collect_manifests import ManifestRecord
from collectors.collector import Collector, FilesCollection
from constants import FileType
from extractors.data_files_extractor import DataFilesExtractor
from collectors.utils import format_partition
from base_classes.utils import timed


@dataclass
class HiddenDataFileMetadata(HiddenFile):
    pointing_manifests: list[Dict[str, str]]


@dataclass
class DataFileRecord(BaseFile):
    format: str
    size_gb: str
    row_count: int
    partition: str
    earliest_appearing_snapshot_id: int
    earliest_appearing_snapshot_timestamp: Optional[str]
    sort_order_id: int
    split_offsets: List[int]
    key_metadata: str
    equality_ids: str
    hidden_data_file_metadata: HiddenDataFileMetadata


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

        self._data_files = [self._process_data_file_row(data_file_row) for data_file_row in data_files_rows]

        return FilesCollection(files=self._data_files)

    def _process_data_file_row(self, data_file_row) -> DataFileRecord:
        data_file_dict = data_file_row.asDict(recursive=True)

        return DataFileRecord(
            type=self._detect_file_type(data_file_dict["content"]),
            file_path=data_file_dict["file_path"],
            format=data_file_dict["file_format"],
            size_gb=f"{(data_file_dict['file_size_in_bytes'] / 1024**3):.10f}",
            row_count=data_file_dict["record_count"],
            partition=format_partition(data_file_dict["partition"]),
            earliest_appearing_snapshot_id=data_file_dict["earliest_snapshot_id"],
            earliest_appearing_snapshot_timestamp=data_file_dict["earliest_snapshot_timestamp"],
            sort_order_id=data_file_dict["sort_order_id"],
            split_offsets=data_file_dict["split_offsets"] or [],
            key_metadata=data_file_dict["key_metadata"],
            equality_ids=data_file_dict["equality_ids"],
            child_files=[],
            hidden_data_file_metadata=HiddenDataFileMetadata(
                pointing_manifests=data_file_dict["pointing_manifests"],
            ),
        )

    @staticmethod
    def _detect_file_type(content: int) -> FileType:
        if content == 0:
            return FileType.DATA

        if content == 1:
            return FileType.POSITION_DELETE

        return FileType.EQUALITY_DELETE
