from collections import defaultdict

from constants import FileType
from partition_files.constants import FILE_CONTENT_EQUALITY_DELETES, FILE_CONTENT_POSITION_DELETES
from partition_files.partition_file_records import PartitionFileRecord


class DeleteFileMatcher:
    def __init__(self, records: list[PartitionFileRecord], unpartitioned_spec_ids: set[int]):
        self._records = records
        self._unpartitioned_spec_ids = unpartitioned_spec_ids
        self._data_files = [record for record in records if record.type == FileType.DATA]
        self._data_files_by_partition: dict[str, list[PartitionFileRecord]] = defaultdict(list)
        self._data_files_by_path: dict[str, PartitionFileRecord] = {}
        for data_file in self._data_files:
            self._data_files_by_partition[data_file.hidden_data_file_metadata.partition_key].append(data_file)
            self._data_files_by_path[data_file.file_path] = data_file

    def match(self) -> None:
        for delete_file in self._records:
            if delete_file.type == FileType.DATA:
                continue

            for data_file in self._affected_data_files(delete_file):
                delete_file.applies_to_data_files.append(data_file.node_id)
                data_file.delete_files.append(delete_file.node_id)

    def _affected_data_files(self, delete_file: PartitionFileRecord) -> list[PartitionFileRecord]:
        content = delete_file.hidden_data_file_metadata.content
        delete_sequence_number = delete_file.data_sequence_number
        referenced_data_file = delete_file.referenced_data_file

        if content == FILE_CONTENT_POSITION_DELETES:
            if referenced_data_file is not None:
                candidates = [self._data_files_by_path[referenced_data_file]] if referenced_data_file in self._data_files_by_path else []
            else:
                candidates = self._data_files_by_partition.get(delete_file.hidden_data_file_metadata.partition_key, [])

            return [data_file for data_file in candidates if data_file.data_sequence_number <= delete_sequence_number]

        if content == FILE_CONTENT_EQUALITY_DELETES:
            if delete_file.partition_spec_id in self._unpartitioned_spec_ids:
                candidates = self._data_files
            else:
                candidates = self._data_files_by_partition.get(delete_file.hidden_data_file_metadata.partition_key, [])

            return [data_file for data_file in candidates if data_file.data_sequence_number < delete_sequence_number]

        return []
