from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from base_classes.spark_table_action import SparkTableAction
from base_classes.utils import timed
from collectors.collect_data_files import CollectDataFiles, DataFileRecord
from collectors.collect_manifests import CollectManifests, ManifestRecord
from collectors.collect_metadata import CollectMetadata, MetadataFileRecord
from collectors.collect_snapshots import CollectSnapshots, SnapshotRecord
from constants import (
    DATA_FILES_CUTOFF_MANIFEST_WARNING,
    DATA_FILES_CUTOFF_WARNING,
    STAGE_COLLECT_DATA_FILES,
    STAGE_COLLECT_MANIFESTS,
    STAGE_COLLECT_METADATA_FILES,
    STAGE_COLLECT_SNAPSHOTS,
    FileType,
)
from env import Env
from icegraph_logger import logger
from iceberg_ports.readable_metrics import ReadableMetricsConverter
from search_cutoff.find_search_cutoff import SearchCutoff, find_search_cutoff
from table_inventory.utils import format_schemas_to_full_dict, get_json_metadata_from_path, parse_json_string_fields


@dataclass
class TableInventoryResult:
    errors: Dict[str, str]
    warnings: Dict[str, str]
    snapshots: List[SnapshotRecord]
    manifests: List[ManifestRecord]
    data_files: List[DataFileRecord]
    metadata_files: List[MetadataFileRecord]
    current_table_specs: Dict[str, Any]


class TableInventory(SparkTableAction):
    def __init__(
        self,
        full_table_name: str,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
        on_stage: Optional[Callable[[str, str], None]] = None,
    ):
        super().__init__(full_table_name)

        self._start_snapshot_id = start_snapshot_id
        self._end_snapshot_id = end_snapshot_id
        self._on_stage = on_stage or (lambda stage, status: None)

        self._errors: Dict[str, str] = {}
        self._warnings: Dict[str, str] = {}

        self._search_cutoff: SearchCutoff = None

        self._metadata_files: List[MetadataFileRecord] = []
        self._snapshots: List[SnapshotRecord] = []
        self._manifests: List[ManifestRecord] = []
        self._data_files: List[DataFileRecord] = []

        self._current_table_specs: Dict[str, Any] = {}

    @timed
    def build(self):
        self._on_stage_start(STAGE_COLLECT_SNAPSHOTS)
        self._find_search_cutoff()
        self._collect_and_set_snapshots()
        self._on_stage_end(STAGE_COLLECT_SNAPSHOTS)

        self._collect_metadata_manifests_and_data_files()

        self._attach_snapshot_files_to_manifest_files()
        self._attach_manifest_files_to_data_files()

        self._warn_if_data_cutoff_happened()

        self._set_current_table_specs()
        self._set_data_file_readable_metrics()
        self._collect_file_errors()

        return TableInventoryResult(
            errors=self._errors,
            warnings=self._warnings,
            snapshots=self._snapshots,
            manifests=self._manifests,
            data_files=self._data_files,
            metadata_files=self._metadata_files,
            current_table_specs=self._current_table_specs,
        )

    def _find_search_cutoff(self):
        self._search_cutoff = find_search_cutoff(
            self._spark,
            self._table_name,
            self._start_snapshot_id,
            self._end_snapshot_id,
        )

    def _collect_and_set_snapshots(self):
        snapshot_collection = CollectSnapshots(
            self._table_name,
            self._search_cutoff.start_snapshot_cutoff,
            self._search_cutoff.end_snapshot_cutoff,
        ).collect()

        self._errors.update(snapshot_collection.errors)

        self._snapshots = snapshot_collection.files

    def _collect_metadata_manifests_and_data_files(self):
        with ThreadPoolExecutor(max_workers=2) as executor:
            metadata_future = executor.submit(self._threaded_collect_metadata_files)
            manifests_and_data_files_future = executor.submit(self._threaded_collect_manifests_and_data_files)

            try:
                metadata_collection = metadata_future.result()

                self._errors.update(metadata_collection.errors)
                self._warnings.update(metadata_collection.warnings)

                self._metadata_files = metadata_collection.files

            except Exception as e:
                logger.error(f"[{self._table_name}] Failed to collect metadata", exc_info=True)
                self._errors["collect_metadata_files"] = str(e)

            try:
                manifests_collection, data_files_collection = manifests_and_data_files_future.result()

                self._errors.update(manifests_collection.errors)
                self._errors.update(data_files_collection.errors)

                self._manifests = manifests_collection.files
                self._data_files = data_files_collection.files

            except Exception as e:
                logger.error(
                    f"[{self._table_name}] Failed to collect manifests or data files",
                    exc_info=True,
                )
                self._errors["collect_manifests_and_data_files"] = str(e)

    def _threaded_collect_metadata_files(self):
        self._on_stage_start(STAGE_COLLECT_METADATA_FILES)
        result = CollectMetadata(
            self._table_name,
            self._search_cutoff.start_metadata_cutoff,
            self._search_cutoff.end_metadata_cutoff,
            self._snapshots,
        ).collect()
        self._on_stage_end(STAGE_COLLECT_METADATA_FILES)

        return result

    def _threaded_collect_manifests_and_data_files(self):
        self._on_stage_start(STAGE_COLLECT_MANIFESTS)
        manifests_collection = CollectManifests(
            self._table_name,
            self._snapshots,
            self._search_cutoff.manifests_to_ignore_df,
        ).collect()
        self._on_stage_end(STAGE_COLLECT_MANIFESTS)

        self._on_stage_start(STAGE_COLLECT_DATA_FILES)
        data_files_collection = CollectDataFiles(
            self._table_name,
            manifests_collection.files,
        ).collect()
        self._on_stage_end(STAGE_COLLECT_DATA_FILES)

        return manifests_collection, data_files_collection

    def _on_stage_start(self, stage_name: str) -> None:
        self._on_stage(stage_name, "in_progress")

    def _on_stage_end(self, stage_name: str) -> None:
        self._on_stage(stage_name, "done")

    def _attach_snapshot_files_to_manifest_files(self):
        if not self._snapshots or not self._manifests:
            return

        snapshot_id_to_snapshot_file_map = {snapshot.snapshot_id: snapshot for snapshot in self._snapshots}

        for manifest in self._manifests:
            for snapshot_id in manifest.hidden_manifest_data.pointing_snapshots:
                snapshot = snapshot_id_to_snapshot_file_map.get(snapshot_id)
                if not snapshot:
                    self._errors[f"Linking {snapshot_id} -> {manifest.file_path}"] = "Snapshot not found"

                else:
                    snapshot.child_files.append(manifest.file_path)

    def _attach_manifest_files_to_data_files(self):
        if not self._manifests or not self._data_files:
            return

        manifest_file_path_to_manifest_map = {manifest.file_path: manifest for manifest in self._manifests}

        for data_file in self._data_files:
            for pointing_manifest in data_file.hidden_data_file_metadata.pointing_manifests:
                manifest_file_path, manifest_pointing_status = (
                    pointing_manifest["path"],
                    pointing_manifest["status"],
                )

                manifest = manifest_file_path_to_manifest_map.get(manifest_file_path)
                if not manifest:
                    self._errors[f"Linking {manifest_file_path} -> {data_file.file_path}"] = "Manifest not found"

                else:
                    manifest.partitions.add(data_file.partition)
                    manifest.total_rows_in_downstream_files += data_file.row_count

                    manifest.child_files.append(data_file.file_path)
                    if manifest_pointing_status == 2:
                        manifest.deleted_child_files.append(data_file.file_path)
                    else:
                        manifest.existing_child_files.append(data_file.file_path)

    def _warn_if_data_cutoff_happened(self):
        if not self._manifests:
            return

        max_manifest_added_snapshot_timestamp = None
        max_manifest_added_snapshot_id = None

        for manifest in self._manifests:
            if manifest.child_files or manifest.error or manifest.added_snapshot_timestamp is None:
                continue

            manifest.warning = DATA_FILES_CUTOFF_MANIFEST_WARNING.format(max_data_files_to_collect=Env.MAX_DATA_FILES_TO_COLLECT)

            if max_manifest_added_snapshot_timestamp is None or max_manifest_added_snapshot_timestamp < manifest.added_snapshot_timestamp:
                max_manifest_added_snapshot_timestamp = manifest.added_snapshot_timestamp
                max_manifest_added_snapshot_id = manifest.added_snapshot_id

        if max_manifest_added_snapshot_timestamp is not None:
            self._warnings["data_files_cutoff"] = DATA_FILES_CUTOFF_WARNING.format(
                max_data_files_to_collect=Env.MAX_DATA_FILES_TO_COLLECT,
                added_snapshot_id=max_manifest_added_snapshot_id,
                added_snapshot_timestamp=max_manifest_added_snapshot_timestamp,
            )

    def _collect_file_errors(self):
        file_groups = (self._metadata_files, self._snapshots, self._manifests, self._data_files)
        for files in file_groups:
            self._errors.update({file.file_path: file.error for file in files if file.error})

    def _set_current_table_specs(self):
        self._current_table_specs = {"table-name": self._table_name}

        try:
            current_main_metadata_file = next(metadata_file for metadata_file in self._metadata_files if metadata_file.type == FileType.MAIN_METADATA)

            current_table_specs = get_json_metadata_from_path(current_main_metadata_file.file_path)
            current_table_specs["schemas"] = format_schemas_to_full_dict(current_table_specs.get("schemas", []))
            current_table_specs = parse_json_string_fields(current_table_specs, ["refs", "properties"])

            self._current_table_specs.update(current_table_specs)

        except Exception as e:
            logger.error(
                f"[{self._table_name}] Metadata specs error for main metadata file path reading",
                exc_info=True,
            )
            self._errors["collect_current_table_specs"] = f"Metadata specs error: {e}"

    def _set_data_file_readable_metrics(self):
        if not self._data_files:
            return

        try:
            current_schema_id = self._current_table_specs["current-schema-id"]
            converter = ReadableMetricsConverter(current_schema_id, self._current_table_specs["schemas"])

            for data_file in self._data_files:
                try:
                    data_file.readable_metrics = converter.convert(data_file.hidden_data_file_metadata.raw_metrics)
                except Exception as e:
                    msg = f"Failed to build readable metrics: {type(e).__name__}: {e}"
                    logger.error(f"[{self._table_name}] {msg} for {data_file.file_path}", exc_info=True)
                    data_file.warning = msg

        except Exception as e:
            logger.error(f"[{self._table_name}] Failed to build readable data file metrics", exc_info=True)
            self._errors["build_readable_metrics"] = str(e)
