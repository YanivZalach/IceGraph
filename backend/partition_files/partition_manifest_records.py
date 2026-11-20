from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

from collectors.collect_manifests import HiddenManifestMetadata, ManifestRecord
from constants import FileType
from partition_files.constants import PARTITION_MATCH_MATCHED, PARTITION_MATCH_UNDECIDED
from partition_files.manifest_pruning import ManifestCandidate
from partition_files.partition_file_records import PartitionFileRecord
from partition_files.partition_table_metadata import PartitionTableMetadata


@dataclass
class PartitionManifestRecord(ManifestRecord):
    content: int
    partition_spec_id: int
    sequence_number: Optional[int]
    partition_match: str


class PartitionManifestRecordBuilder:
    def __init__(self, metadata: PartitionTableMetadata, snapshot_id: int):
        self._metadata = metadata
        self._snapshot_id = snapshot_id

    def build(
        self, manifests: list[ManifestCandidate], records: list[PartitionFileRecord], manifest_errors: dict[str, list[str]]
    ) -> list[PartitionManifestRecord]:
        records_by_manifest: dict[str, list[PartitionFileRecord]] = defaultdict(list)
        for record in records:
            for pointing_manifest in record.hidden_data_file_metadata.pointing_manifests:
                records_by_manifest[pointing_manifest["path"]].append(record)

        manifest_records = []
        for manifest in manifests:
            children = records_by_manifest.get(manifest.manifest_path, [])
            errors = manifest_errors.get(manifest.manifest_path, [])
            if not children and not errors:
                continue

            manifest_records.append(self._build_manifest_record(manifest, children, errors))

        return manifest_records

    def _build_manifest_record(self, manifest: ManifestCandidate, children: list[PartitionFileRecord], errors: list[str]) -> PartitionManifestRecord:
        partitions = []
        for child in children:
            if child.partition not in partitions:
                partitions.append(child.partition)

        child_ids = [child.node_id for child in children]

        return PartitionManifestRecord(
            type=FileType.MANIFEST,
            file_path=manifest.manifest_path,
            added_snapshot_id=manifest.added_snapshot_id,
            added_snapshot_timestamp=self._metadata.snapshot_timestamps.get(manifest.added_snapshot_id),
            first_row_id=None,
            first_row_id_by_snapshot=None,
            partitions=partitions,
            total_rows_in_downstream_files=sum(child.row_count or 0 for child in children),
            existing_child_files=child_ids,
            deleted_child_files=[],
            child_files=child_ids,
            content=manifest.content,
            partition_spec_id=manifest.spec_filter.spec_id,
            sequence_number=manifest.sequence_number,
            partition_match=PARTITION_MATCH_MATCHED if manifest.spec_filter.is_complete else PARTITION_MATCH_UNDECIDED,
            hidden_manifest_data=HiddenManifestMetadata(pointing_snapshots=[self._snapshot_id], row_id_assignments=[]),
            errors=list(errors),
        )
