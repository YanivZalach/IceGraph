from base_classes.utils import column_to_string_utc
import json
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Dict, List, Optional

import pyspark.sql
from arrow import Arrow
from pyspark.sql import functions as F

from base_classes.base_file import BaseFile
from collectors.collect_snapshots import SnapshotRecord
from collectors.collector import Collector, FilesCollection
from constants import FileType, MAIN_BRANCH_ICEBERG_TABLE_NAME
from icegraph_logger import logger
from collectors.utils import get_metadata_row_slim_df_from_path
from base_classes.utils import timed


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
    pointed_metadata_log_count: Optional[int]


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

        self._metadata_files: List[MetadataFileRecord] = []
        self._bad_metadata_files: List[MetadataFileRecord] = []
        self._errors: Dict[str, str] = {}

    @timed
    def collect(self) -> FilesCollection:
        try:
            self._ordered_metadata_to_timestamp = self._query_metadata_files()
            metadata_files_df = self._build_metadata_files_df()

            if metadata_files_df is not None:
                snap_id_to_path = self._get_snap_id_to_path()

                rows = metadata_files_df.collect()
                for index, row in enumerate(rows):
                    row_dict = row.asDict(recursive=True)
                    metadata_file_type = FileType.MAIN_METADATA if index == 0 else FileType.METADATA

                    self._metadata_files.append(self._parse_metadata_row(metadata_file_type, row_dict, snap_id_to_path))

                self._add_bad_metadata_files()

        except Exception as e:
            logger.error(f"[{self._table_name}] metadata collection failed", exc_info=True)
            self._errors["metadata_collection"] = str(e)

        return FilesCollection(files=self._metadata_files, errors=self._errors)

    @cached_property
    def _ordered_metadata_paths(self) -> list[str]:
        return list(self._ordered_metadata_to_timestamp.keys())

    def _query_metadata_files(self) -> dict:
        metadata_df = (
            self._spark.sql(f"SELECT * FROM {self._table_name}.metadata_log_entries")
            .withColumnRenamed("timestamp", "metadata_timestamp")
            .select("file", "metadata_timestamp")
            .filter(F.col("metadata_timestamp") >= F.lit(str(self._start_metadata_cutoff)))
            .filter(F.col("metadata_timestamp") <= F.lit(str(self._end_metadata_cutoff)))
            .orderBy(F.desc("metadata_timestamp"))
            .withColumn("metadata_timestamp", column_to_string_utc("metadata_timestamp"))
        )
        return {row.file: row.metadata_timestamp for row in metadata_df.collect()}

    def _build_metadata_files_df(self) -> Optional[pyspark.sql.DataFrame]:
        metadata_files_df = None
        for file, timestamp in self._ordered_metadata_to_timestamp.items():
            try:
                df = get_metadata_row_slim_df_from_path(file).withColumn("metadata_timestamp", F.lit(timestamp)).withColumn("file", F.lit(file))
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
                        error=str(e),
                        timestamp=timestamp,
                        snapshot_id=None,
                        previous_file=None,
                        last_sequence_number=None,
                        partition_spec_id=None,
                        current_schema_id=None,
                        sort_order_id=None,
                        refs={},
                        properties={},
                        pointed_snapshots_files=None,
                        pointed_metadata_log_count=None,
                    )
                )

        return metadata_files_df

    def _add_bad_metadata_files(self) -> None:
        for bad_file in self._bad_metadata_files:
            index_to_insert = len(self._metadata_files)

            for index, metadata_file in enumerate(self._metadata_files):
                if metadata_file.previous_file == bad_file.file_path:
                    index_to_insert = index + 1
                    break

            self._metadata_files.insert(index_to_insert, bad_file)

    def _get_snap_id_to_path(self) -> Dict[int, str]:
        return {s.snapshot_id: s.file_path for s in (self._snapshots or [])}

    def _get_previous_metadata_file(self, file_path: str) -> Optional[str]:
        file_index = self._ordered_metadata_paths.index(file_path)

        return self._ordered_metadata_paths[file_index - 1] if file_index - 1 >= 0 else None

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

    def _parse_metadata_row(self, file_type: FileType, row: dict, snap_id_to_path: dict) -> MetadataFileRecord:
        refs = self._parse_refs(row)
        branches_child_files = self._build_branches_child_files(refs, snap_id_to_path)

        current_snap_path = snap_id_to_path.get(row["current-snapshot-id"])
        child_files = ([current_snap_path] if current_snap_path else []) + branches_child_files

        return MetadataFileRecord(
            type=file_type,
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
            pointed_metadata_log_count=row["pointed_metadata_log_count"],
            child_files=child_files,
        )
