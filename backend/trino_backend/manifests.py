from dataclasses import dataclass
from typing import Dict, List, Optional

from base_classes.utils import timed
from collectors.collect_manifests import CollectManifests, HiddenManifestMetadata, ManifestRecord
from collectors.collector import FilesCollection
from constants import MANIFESTS_LIMIT_ERROR
from env import Env
from trino_backend.manifest_files import MANIFEST_LIST_FILES
from trino_backend.raw_files import MISSING_FILE_ERROR, find_existing_files, read_raw_files
from trino_backend.trino_client import Row


@dataclass
class TrinoHiddenManifestMetadata(HiddenManifestMetadata):
    partition_spec_id: Optional[int]


class TrinoCollectManifests(CollectManifests):
    @timed
    def collect(self) -> FilesCollection:
        manifests_rows = self._read_manifest_rows()

        if len(manifests_rows) > Env.MAX_MANIFESTS_TO_COLLECT:
            raise ValueError(MANIFESTS_LIMIT_ERROR.format(max_manifests_to_collect=Env.MAX_MANIFESTS_TO_COLLECT))

        snapshot_timestamps = {snapshot.snapshot_id: snapshot.timestamp for snapshot in self._snapshots}
        self._manifests = [self._process_trino_manifest_row(manifest_row, snapshot_timestamps) for manifest_row in manifests_rows]

        return FilesCollection(files=self._manifests)

    def _process_trino_manifest_row(self, manifest_row: Row, snapshot_timestamps: Dict[int, str]) -> ManifestRecord:
        manifest = self._process_manifest_row(manifest_row, snapshot_timestamps)
        hidden_manifest_data = manifest.hidden_manifest_data
        manifest.hidden_manifest_data = TrinoHiddenManifestMetadata(
            pointing_snapshots=hidden_manifest_data.pointing_snapshots,
            row_id_assignments=hidden_manifest_data.row_id_assignments,
            partition_spec_id=manifest_row.partition_spec_id,
        )

        return manifest

    def _read_manifest_rows(self) -> List[Row]:
        snapshots_by_manifest_list = {snapshot.file_path: snapshot for snapshot in self._snapshots}
        snapshot_timestamps = {snapshot.snapshot_id: snapshot.timestamp for snapshot in self._snapshots}
        manifest_list_rows = read_raw_files(
            MANIFEST_LIST_FILES, snapshots_by_manifest_list, "manifest_path, partition_spec_id, added_snapshot_id, first_row_id"
        )
        self._report_missing_manifest_lists(snapshots_by_manifest_list, {row.file_path for row in manifest_list_rows})

        manifests: Dict[str, Row] = {}
        for manifest_list_row in manifest_list_rows:
            if manifest_list_row.manifest_path in self._manifests_to_ignore_df:
                continue

            snapshot_id = snapshots_by_manifest_list[manifest_list_row.file_path].snapshot_id
            manifest = manifests.setdefault(
                manifest_list_row.manifest_path,
                Row(
                    path=manifest_list_row.manifest_path,
                    added_snapshot_id=manifest_list_row.added_snapshot_id,
                    partition_spec_id=manifest_list_row.partition_spec_id,
                    added_snapshot_timestamp=snapshot_timestamps.get(manifest_list_row.added_snapshot_id),
                    snapshot_ids=[],
                    snapshot_first_row_ids=[],
                ),
            )
            manifest.snapshot_ids.append(snapshot_id)
            if manifest_list_row.first_row_id is not None:
                manifest.snapshot_first_row_ids.append({"snapshot_id": snapshot_id, "first_row_id": manifest_list_row.first_row_id})

        manifests_by_path = sorted(manifests.values(), key=lambda manifest: manifest.path)
        ordered_manifests = sorted(manifests_by_path, key=lambda manifest: manifest.added_snapshot_timestamp or "", reverse=True)

        return ordered_manifests[: Env.MAX_MANIFESTS_TO_COLLECT + 1]

    def _report_missing_manifest_lists(self, snapshots_by_manifest_list: Dict, read_manifest_lists: set) -> None:
        unread_manifest_lists = set(snapshots_by_manifest_list) - read_manifest_lists
        existing_manifest_lists = find_existing_files(unread_manifest_lists)
        for manifest_list in unread_manifest_lists - existing_manifest_lists:
            snapshots_by_manifest_list[manifest_list].errors.append(MISSING_FILE_ERROR.format(file_path=manifest_list))
