from base_classes.utils import format_snapshot_summary
from collectors.collect_snapshots import SnapshotRecord
from constants import FileType
from partition_files.partition_manifest_records import PartitionManifestRecord
from partition_files.partition_table_metadata import PartitionTableMetadata
from partition_files.utils import format_epoch_millis


class PartitionSnapshotRecordBuilder:
    def __init__(self, metadata: PartitionTableMetadata):
        self._metadata = metadata

    def build(self, manifests: list[PartitionManifestRecord], errors: list[str]) -> SnapshotRecord:
        snapshot = self._metadata.snapshot
        summary = {key: value for key, value in (snapshot.get("summary") or {}).items() if key != "operation"}
        operation = (snapshot.get("summary") or {}).get("operation")
        snapshot_record = SnapshotRecord(
            type=FileType.SNAPSHOT,
            file_path=snapshot["manifest-list"],
            timestamp=format_epoch_millis(snapshot.get("timestamp-ms")),
            snapshot_id=snapshot["snapshot-id"],
            action_link=None,
            parent_id=snapshot.get("parent-snapshot-id"),
            operation=operation,
            operation_description=operation,
            summary=format_snapshot_summary(summary),
            first_row_id=snapshot.get("first-row-id"),
            added_rows=snapshot.get("added-rows"),
            child_files=[manifest.file_path for manifest in manifests],
        )
        snapshot_record.errors.extend(errors)

        return snapshot_record
