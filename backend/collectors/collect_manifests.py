from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyspark

from base_classes.base_file import BaseFile, HiddenFile
from base_classes.utils import timed
from collectors.collect_snapshots import SnapshotRecord
from collectors.collector import Collector, FilesCollection
from collectors.row_lineage import RowIdAssignment, build_row_id_fields, earliest_assignment_per_value
from constants import MANIFESTS_LIMIT_ERROR, FileType
from env import Env
from extractors.manifests_extractor import ManifestsExtractor


@dataclass
class HiddenManifestMetadata(HiddenFile):
    pointing_snapshots: list[int]
    row_id_assignments: list[RowIdAssignment]
    partition_spec_id: Optional[int]


@dataclass
class ManifestRecord(BaseFile):
    added_snapshot_id: int
    added_snapshot_timestamp: Optional[str]
    first_row_id: Optional[int]
    first_row_id_by_snapshot: Optional[Dict[str, int]]
    partitions: List[List[Dict[str, Any]]]
    total_rows_in_downstream_files: int
    existing_child_files: list[str]
    deleted_child_files: list[str]
    hidden_manifest_data: HiddenManifestMetadata


class CollectManifests(Collector):
    def __init__(
        self,
        full_table_name: str,
        snapshots: list[SnapshotRecord],
        manifests_to_ignore_df: pyspark.sql.DataFrame,
    ):
        super().__init__(full_table_name)
        self._snapshots = snapshots
        self._manifests_to_ignore_df = manifests_to_ignore_df

        self._manifests: List[ManifestRecord] = []

    @timed
    def collect(self) -> FilesCollection:
        manifests_rows = ManifestsExtractor(self._table_name, self._snapshots, self._manifests_to_ignore_df).extract_dataframe().collect()

        if len(manifests_rows) > Env.MAX_MANIFESTS_TO_COLLECT:
            raise ValueError(MANIFESTS_LIMIT_ERROR.format(max_manifests_to_collect=Env.MAX_MANIFESTS_TO_COLLECT))

        snapshot_timestamps = {snapshot.snapshot_id: snapshot.timestamp for snapshot in self._snapshots}
        self._manifests = [self._process_manifest_row(manifest_row, snapshot_timestamps) for manifest_row in manifests_rows]

        return FilesCollection(files=self._manifests)

    @staticmethod
    def _process_manifest_row(manifest_row, snapshot_timestamps: Dict[int, str]) -> ManifestRecord:
        manifest_dict = manifest_row.asDict(recursive=True)
        row_id_assignments = earliest_assignment_per_value(
            [
                RowIdAssignment(entry["snapshot_id"], snapshot_timestamps[entry["snapshot_id"]], entry["first_row_id"])
                for entry in manifest_dict["snapshot_first_row_ids"]
                if entry["first_row_id"] is not None
            ]
        )
        row_id_fields = build_row_id_fields(row_id_assignments)

        return ManifestRecord(
            type=FileType.MANIFEST,
            file_path=manifest_dict["path"],
            added_snapshot_id=manifest_dict["added_snapshot_id"],
            added_snapshot_timestamp=manifest_dict["added_snapshot_timestamp"],
            first_row_id=row_id_fields.first_row_id,
            first_row_id_by_snapshot=row_id_fields.first_row_id_by_snapshot,
            partitions=[],
            total_rows_in_downstream_files=0,
            existing_child_files=[],
            deleted_child_files=[],
            child_files=[],
            hidden_manifest_data=HiddenManifestMetadata(
                pointing_snapshots=manifest_dict["snapshot_ids"],
                row_id_assignments=row_id_assignments,
                partition_spec_id=manifest_dict["partition_spec_id"],
            ),
        )
