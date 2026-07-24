from base_classes.utils import column_to_string_utc
import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pyspark.sql
from arrow import Arrow
from pyspark.sql import functions as F

from base_classes.base_file import BaseFile, HiddenFile
from collectors.collect_snapshots import SnapshotRecord
from collectors.collector import Collector, FilesCollection
from constants import FileType, MAIN_BRANCH_ICEBERG_TABLE_NAME
from icegraph_logger import logger
from collectors.utils import get_metadata_row_slim_df_from_path
from base_classes.utils import timed


@dataclass
class HiddenMetadata2(HiddenFile):
    color_shift: float
    branch_files: Dict[str, str]
    main_branch_file: str


@dataclass
class MetadataFileRecord(BaseFile):
    timestamp: str
    snapshot_id: Optional[int]
    previous_file: Optional[str]
    last_sequence_number: Optional[int]
    partition_spec_id: int
    current_schema_id: int
    sort_order_id: int
    refs: Dict[str, Any]
    properties: Dict[str, str]
    pointed_snapshots_files: Optional[str]
    pointed_metadata_log_count: int
    hidden_metadata: HiddenMetadata2


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

        self._metadata_files: List[MetadataFileRecord] = []
        self._errors: Dict[str, str] = {}

    @timed
    def collect(self) -> FilesCollection:
        try:
            metadata_files = self._query_metadata_files()
            metadata_files_df = self._build_metadata_files_df(metadata_files)

            if metadata_files_df is not None:
                snap_id_to_path = self._get_snap_id_to_path()

                rows = metadata_files_df.orderBy(F.desc("metadata_timestamp")).collect()
                self._metadata_files = [
                    self._parse_metadata_row(index, row.asDict(recursive=True), rows, snap_id_to_path) for index, row in enumerate(rows)
                ]

        except Exception as e:
            logger.error(f"[{self._table_name}] metadata collection failed", exc_info=True)
            self._errors["metadata_collection"] = str(e)

        return FilesCollection(files=self._metadata_files, errors=self._errors)

    def _query_metadata_files(self) -> dict:
        metadata_df = (
            self._spark.sql(f"SELECT * FROM {self._table_name}.metadata_log_entries")
            .withColumnRenamed("timestamp", "metadata_timestamp")
            .select("file", "metadata_timestamp")
            .filter(F.col("metadata_timestamp") >= F.lit(str(self._start_metadata_cutoff)))
            .filter(F.col("metadata_timestamp") <= F.lit(str(self._end_metadata_cutoff)))
            .withColumn("metadata_timestamp", column_to_string_utc("metadata_timestamp"))
        )
        return {row.file: row.metadata_timestamp for row in metadata_df.collect()}

    def _build_metadata_files_df(self, metadata_files: dict) -> Optional[pyspark.sql.DataFrame]:
        metadata_files_df = None
        for file, timestamp in metadata_files.items():
            try:
                df = get_metadata_row_slim_df_from_path(file).withColumn("metadata_timestamp", F.lit(timestamp)).withColumn("file", F.lit(file))
                metadata_files_df = df if metadata_files_df is None else metadata_files_df.unionByName(df, allowMissingColumns=True)

            except Exception as e:
                logger.error(
                    f"[{self._table_name}] Metadata file read error for {file}",
                    exc_info=True,
                )
                self._errors[file] = f"Metadata file read error: {e}"

        return metadata_files_df

    def _get_snap_id_to_path(self) -> Dict[int, str]:
        return {s.snapshot_id: s.file_path for s in (self._snapshots or [])}

    @staticmethod
    def _parse_refs(row: dict) -> dict:
        return json.loads(row["refs"]) if row.get("refs") else {}

    @staticmethod
    def _build_branch_files(refs: dict, snap_id_to_path: dict) -> dict:
        branches = {
            name: attrs["snapshot-id"] for name, attrs in refs.items() if attrs.get("type") == "branch" and name != MAIN_BRANCH_ICEBERG_TABLE_NAME
        }

        snapshot_to_branches = defaultdict(list)
        for branch_name, snap_id in branches.items():
            snapshot_to_branches[snap_id].append(branch_name)

        child_files = []
        branch_files = {}
        for snap_id, branch_names in snapshot_to_branches.items():
            snap_path = snap_id_to_path.get(snap_id)
            if snap_path:
                child_files.append(snap_path)
                branch_files[snap_path] = ", ".join(branch_names)

        return {"branches_child_files": child_files, "branch_files": branch_files}

    def _parse_metadata_row(self, index: int, row: dict, rows: list, snap_id_to_path: dict) -> MetadataFileRecord:
        number_of_rows = len(rows)
        file_type = FileType.MAIN_METADATA if index == 0 else FileType.METADATA

        refs = self._parse_refs(row)
        branch_files_build = self._build_branch_files(refs, snap_id_to_path)
        branches_child_files, branch_files = (
            branch_files_build["branches_child_files"],
            branch_files_build["branch_files"],
        )

        current_snap_path = snap_id_to_path.get(row["current-snapshot-id"])
        child_files = ([current_snap_path] if current_snap_path else []) + branches_child_files

        return MetadataFileRecord(
            type=file_type,
            file_path=row["file"],
            timestamp=str(row["metadata_timestamp"]),
            snapshot_id=row["current-snapshot-id"],
            previous_file=(rows[index + 1]["file"] if index + 1 < number_of_rows else None),
            last_sequence_number=(row["last-sequence-number"] if "last-sequence-number" in row else None),
            partition_spec_id=row["default-spec-id"],
            current_schema_id=row["current-schema-id"],
            sort_order_id=row["default-sort-order-id"],
            refs=refs,
            properties=json.loads(row["properties"]) if row.get("properties") else {},
            pointed_snapshots_files=row.get("pointed_snapshots_files"),
            pointed_metadata_log_count=row["pointed_metadata_log_count"],
            child_files=child_files,
            hidden_metadata=HiddenMetadata2(
                color_shift=1 - index / (1.5 * number_of_rows), branch_files=branch_files, main_branch_file=current_snap_path
            ),
        )
