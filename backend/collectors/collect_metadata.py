from base_classes.utils import column_to_string_utc
import json
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Dict, List, Optional, Set

import pyspark.sql
from arrow import Arrow
from pyspark.sql import functions as F

from base_classes.base_file import BaseFile
from collectors.collect_snapshots import SnapshotRecord
from collectors.collector import Collector, FilesCollection
from constants import (
    METADATA_FILES_CUTOFF_WARNING,
    TABLE_STATISTICS_BEFORE_RANGE_WARNING,
    FileType,
    MAIN_BRANCH_ICEBERG_TABLE_NAME,
)
from env import Env
from icegraph_logger import logger
from collectors.utils import get_metadata_row_slim_df_from_path
from collectors.statistics_utils import with_pointed_table_statistics
from base_classes.utils import timed


@dataclass
class MetadataLogSelection:
    ordered_metadata_to_timestamp: Dict[str, str]
    metadata_file_before_range: Optional[str]


@dataclass
class MetadataFileRecord(BaseFile):
    timestamp: Optional[str]
    snapshot_id: Optional[int]
    previous_file: Optional[str]
    last_sequence_number: Optional[int]
    partition_spec_id: Optional[int]
    current_schema_id: Optional[int]
    sort_order_id: Optional[int]
    refs: Dict[str, Any]
    properties: Dict[str, str]
    pointed_snapshots_files: Optional[List[Dict[str, str]]]
    pointed_statistics_files: Optional[Dict[int, str]]
    pointed_metadata_log_count: Optional[int]


@dataclass(frozen=True)
class MetadataFilesCollection(FilesCollection):
    statistics_paths_before_range: Set[str] = field(default_factory=set)


class CollectMetadata(Collector):
    def __init__(
        self,
        full_table_name: str,
        start_metadata_cutoff: Arrow,
        end_metadata_cutoff: Arrow,
        snapshots: List[SnapshotRecord],
    ):
        super().__init__(full_table_name)
        self._snapshots = snapshots

        self._start_metadata_cutoff = start_metadata_cutoff
        self._end_metadata_cutoff = end_metadata_cutoff

        self._ordered_metadata_to_timestamp: Dict[str, str] = {}
        self._metadata_file_before_range: Optional[str] = None

        self._metadata_files: List[MetadataFileRecord] = []
        self._bad_metadata_files: List[MetadataFileRecord] = []
        self._statistics_paths_before_range: Set[str] = set()

        self._errors: Dict[str, List[str]] = {}
        self._warnings: Dict[str, List[str]] = {}

    @timed
    def collect(self) -> MetadataFilesCollection:
        try:
            metadata_log_selection = self._query_metadata_files()
            self._ordered_metadata_to_timestamp = metadata_log_selection.ordered_metadata_to_timestamp
            self._metadata_file_before_range = metadata_log_selection.metadata_file_before_range
            self._apply_metadata_files_cutoff()

            metadata_files_df = self._build_metadata_files_df()

            if metadata_files_df is not None:
                snap_id_to_path = self._get_snap_id_to_path()

                for row in with_pointed_table_statistics(metadata_files_df).collect():
                    self._process_metadata_row(row.asDict(recursive=True), snap_id_to_path)

            self._metadata_files.extend(self._bad_metadata_files)
            self._apply_metadata_order()

        except Exception as e:
            logger.error(f"[{self._table_name}] metadata collection failed", exc_info=True)
            self._errors["metadata_collection"] = [str(e)]

        return MetadataFilesCollection(
            files=self._metadata_files,
            statistics_paths_before_range=self._statistics_paths_before_range,
            errors=self._errors,
            warnings=self._warnings,
        )

    @cached_property
    def _ordered_metadata_paths(self) -> list[str]:
        return list(self._ordered_metadata_to_timestamp.keys())

    def _query_metadata_files(self) -> MetadataLogSelection:
        log_df = (
            self._spark.sql(f"SELECT * FROM {self._table_name}.metadata_log_entries")
            .withColumnRenamed("timestamp", "metadata_timestamp")
            .select("file", "metadata_timestamp")
            .filter(F.col("metadata_timestamp") <= F.lit(str(self._end_metadata_cutoff)))
        )
        start_cutoff = F.lit(str(self._start_metadata_cutoff))
        in_range_df = (
            log_df.filter(F.col("metadata_timestamp") >= start_cutoff)
            .orderBy(F.desc("metadata_timestamp"))
            .limit(Env.MAX_METADATA_FILES_TO_COLLECT + 1)
            .withColumn("is_in_range", F.lit(True))
        )
        before_range_df = (
            log_df.filter(F.col("metadata_timestamp") < start_cutoff)
            .orderBy(F.desc("metadata_timestamp"))
            .limit(1)
            .withColumn("is_in_range", F.lit(False))
        )
        rows = (
            in_range_df.unionByName(before_range_df)
            .orderBy(F.desc("metadata_timestamp"))
            .withColumn("metadata_timestamp", column_to_string_utc("metadata_timestamp"))
            .collect()
        )

        metadata_log_selection = MetadataLogSelection(ordered_metadata_to_timestamp={}, metadata_file_before_range=None)
        for row in rows:
            if row.is_in_range:
                metadata_log_selection.ordered_metadata_to_timestamp[row.file] = row.metadata_timestamp
            else:
                metadata_log_selection.metadata_file_before_range = row.file

        return metadata_log_selection

    def _apply_metadata_files_cutoff(self) -> None:
        if len(self._ordered_metadata_to_timestamp) <= Env.MAX_METADATA_FILES_TO_COLLECT:
            return

        self._metadata_file_before_range = list(self._ordered_metadata_to_timestamp)[Env.MAX_METADATA_FILES_TO_COLLECT]
        self._ordered_metadata_to_timestamp = dict(list(self._ordered_metadata_to_timestamp.items())[: Env.MAX_METADATA_FILES_TO_COLLECT])

        self._warnings["metadata_files_cutoff"] = [
            METADATA_FILES_CUTOFF_WARNING.format(max_metadata_files_to_collect=Env.MAX_METADATA_FILES_TO_COLLECT)
        ]

    def _build_metadata_files_df(self) -> Optional[pyspark.sql.DataFrame]:
        metadata_files_df = None
        for file, timestamp in self._ordered_metadata_to_timestamp.items():
            try:
                df = (
                    get_metadata_row_slim_df_from_path(file)
                    .withColumn("metadata_timestamp", F.lit(timestamp))
                    .withColumn("file", F.lit(file))
                    .withColumn("is_before_range", F.lit(False))
                )
                metadata_files_df = df if metadata_files_df is None else metadata_files_df.unionByName(df, allowMissingColumns=True)

            except Exception as e:
                logger.error(
                    f"[{self._table_name}] Metadata file read error for {file}",
                    exc_info=True,
                )
                self._bad_metadata_files.append(
                    MetadataFileRecord(
                        type=FileType.METADATA,
                        file_path=file,
                        child_files=[],
                        errors=[str(e)],
                        timestamp=timestamp,
                        snapshot_id=None,
                        previous_file=self._get_previous_metadata_file(file),
                        last_sequence_number=None,
                        partition_spec_id=None,
                        current_schema_id=None,
                        sort_order_id=None,
                        refs={},
                        properties={},
                        pointed_snapshots_files=None,
                        pointed_statistics_files=None,
                        pointed_metadata_log_count=None,
                    )
                )

        if metadata_files_df is None:
            return None

        before_range_df = self._build_metadata_file_before_range_df()
        if before_range_df is not None:
            metadata_files_df = metadata_files_df.unionByName(before_range_df, allowMissingColumns=True)

        return metadata_files_df

    def _build_metadata_file_before_range_df(self) -> Optional[pyspark.sql.DataFrame]:
        file = self._metadata_file_before_range
        if file is None:
            return None

        try:
            return get_metadata_row_slim_df_from_path(file).withColumn("file", F.lit(file)).withColumn("is_before_range", F.lit(True))

        except Exception:
            logger.warning(f"[{self._table_name}] Metadata file before range read error for {file}", exc_info=True)
            self._warnings["table_statistics_before_range"] = [TABLE_STATISTICS_BEFORE_RANGE_WARNING.format(metadata_file=file)]
            return None

    def _process_metadata_row(self, row: dict, snap_id_to_path: dict) -> None:
        pointed_statistics = [entry for entry in row["pointed_statistics"] or [] if entry["statistics_path"] is not None]

        if row["is_before_range"]:
            self._statistics_paths_before_range = {entry["statistics_path"] for entry in pointed_statistics}
            return

        self._metadata_files.append(self._parse_metadata_row(row, snap_id_to_path, pointed_statistics))

    def _apply_metadata_order(self) -> None:
        if not self._metadata_files:
            return

        metadata_file_by_path = {metadata_file.file_path: metadata_file for metadata_file in self._metadata_files}

        self._metadata_files = [metadata_file_by_path[file_path] for file_path in self._ordered_metadata_paths]
        self._metadata_files[0].type = FileType.MAIN_METADATA

    def _get_snap_id_to_path(self) -> Dict[int, str]:
        return {s.snapshot_id: s.file_path for s in (self._snapshots or [])}

    def _get_previous_metadata_file(self, file_path: str) -> Optional[str]:
        older_index = self._ordered_metadata_paths.index(file_path) + 1

        return self._ordered_metadata_paths[older_index] if older_index < len(self._ordered_metadata_paths) else None

    @staticmethod
    def _parse_refs(row: dict) -> dict:
        return json.loads(row["refs"]) if row.get("refs") else {}

    @staticmethod
    def _build_branches_child_files(refs: dict, snap_id_to_path: dict) -> List[str]:
        branches_child_files = []
        for branch_name, attrs in refs.items():
            if attrs.get("type") != "branch" or branch_name == MAIN_BRANCH_ICEBERG_TABLE_NAME:
                continue

            snap_path = snap_id_to_path.get(attrs["snapshot-id"])
            if snap_path and snap_path not in branches_child_files:
                branches_child_files.append(snap_path)

        return branches_child_files

    def _parse_metadata_row(self, row: dict, snap_id_to_path: dict, pointed_statistics: List[dict]) -> MetadataFileRecord:
        refs = self._parse_refs(row)
        branches_child_files = self._build_branches_child_files(refs, snap_id_to_path)

        current_snap_path = snap_id_to_path.get(row["current-snapshot-id"])
        child_files = ([current_snap_path] if current_snap_path else []) + branches_child_files

        return MetadataFileRecord(
            type=FileType.METADATA,
            file_path=row["file"],
            timestamp=str(row["metadata_timestamp"]),
            snapshot_id=row["current-snapshot-id"],
            previous_file=self._get_previous_metadata_file(row["file"]),
            last_sequence_number=(row["last-sequence-number"] if "last-sequence-number" in row else None),
            partition_spec_id=row["default-spec-id"],
            current_schema_id=row["current-schema-id"],
            sort_order_id=row["default-sort-order-id"],
            refs=refs,
            properties=json.loads(row["properties"]),
            pointed_snapshots_files=json.loads(row["pointed_snapshots_files"]) if row.get("pointed_snapshots_files") else None,
            pointed_statistics_files={
                entry["snapshot_id"]: entry["statistics_path"] for entry in sorted(pointed_statistics, key=lambda entry: entry["statistics_path"])
            },
            pointed_metadata_log_count=row["pointed_metadata_log_count"],
            child_files=child_files,
        )
