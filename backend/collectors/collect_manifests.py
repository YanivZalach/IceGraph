from dataclasses import dataclass
from typing import List, Optional

import pyspark

from base_classes.base_file import BaseFile, HiddenFile
from base_classes.utils import timed
from collectors.collect_snapshots import SnapshotRecord
from collectors.collector import Collector, FilesCollection
from constants import MANIFESTS_LIMIT_ERROR, FileType
from env import Env
from extractors.manifests_extractor import ManifestsExtractor


@dataclass
class HiddenManifestMetadata(HiddenFile):
    pointing_snapshots: list[int]


@dataclass
class ManifestRecord(BaseFile):
    added_snapshot_id: int
    added_snapshot_timestamp: Optional[str]
    partitions: set
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

        self._manifests = [self._process_manifest_row(manifest_row) for manifest_row in manifests_rows]

        return FilesCollection(files=self._manifests)

    @staticmethod
    def _process_manifest_row(manifest_row) -> ManifestRecord:
        manifest_dict = manifest_row.asDict(recursive=True)

        return ManifestRecord(
            type=FileType.MANIFEST,
            file_path=manifest_dict["path"],
            added_snapshot_id=manifest_dict["added_snapshot_id"],
            added_snapshot_timestamp=manifest_dict["added_snapshot_timestamp"],
            partitions=set(),
            total_rows_in_downstream_files=0,
            existing_child_files=[],
            deleted_child_files=[],
            child_files=[],
            hidden_manifest_data=HiddenManifestMetadata(pointing_snapshots=manifest_dict["snapshot_ids"]),
        )
