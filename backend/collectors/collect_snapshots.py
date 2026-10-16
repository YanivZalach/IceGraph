from dataclasses import dataclass
from typing import Dict, List, Optional

import pyspark.sql
from arrow import Arrow
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

from base_classes.base_file import BaseFile
from base_classes.utils import collect_graph_metadata_file, column_to_string_utc, format_snapshot_summary, timed
from collectors.collector import Collector, FilesCollection
from constants import FileType
from env import Env
from icegraph_logger import logger


@dataclass
class SnapshotRecord(BaseFile):
    timestamp: str
    snapshot_id: int
    action_link: Optional[str]
    parent_id: Optional[int]
    operation: Optional[str]
    operation_description: Optional[str]
    summary: Dict[str, str]
    first_row_id: Optional[int]
    added_rows: Optional[int]


@dataclass
class SnapshotRowLineage:
    first_row_id: Optional[int]
    added_rows: Optional[int]


class CollectSnapshots(Collector):
    def __init__(
        self,
        full_table_name: str,
        start_snapshot_cutoff: Arrow,
        end_snapshot_cutoff: Arrow,
    ):
        super().__init__(full_table_name)

        self._start_snapshot_cutoff = start_snapshot_cutoff
        self._end_snapshot_cutoff = end_snapshot_cutoff

        self._snapshots: List[SnapshotRecord] = []
        self._errors: Dict[str, List[str]] = {}
        self._warnings: Dict[str, List[str]] = {}

    @timed
    def collect(self) -> FilesCollection:
        try:
            snapshots_df = self._query_snapshots_df()
            self._validate_snapshot_count(snapshots_df)
            row_lineage_by_snapshot_id = self._query_row_lineage_by_snapshot_id()
            self._snapshots = []
            inline_manifest_warnings = []
            for row in snapshots_df.collect():
                if not row.manifest_list:
                    inline_manifest_warnings.append(
                        f"Snapshot {row.snapshot_id} has inline v1 manifests, which are not supported by this version of IceGraph."
                    )
                    continue

                self._snapshots.append(self._parse_snapshot_row(row, row_lineage_by_snapshot_id))
            if inline_manifest_warnings:
                self._warnings["inline_manifests"] = inline_manifest_warnings

        except Exception as e:
            logger.error(f"[{self._table_name}] snapshots collection failed", exc_info=True)
            self._errors["snapshot_collection"] = [str(e)]

        return FilesCollection(files=self._snapshots, errors=self._errors, warnings=self._warnings)

    def _query_snapshots_df(self) -> pyspark.sql.DataFrame:
        return (
            self._spark.sql(f"SELECT * FROM {self._table_name}.snapshots ORDER BY committed_at DESC")
            .filter(F.col("committed_at") >= F.lit(str(self._start_snapshot_cutoff)))
            .filter(F.col("committed_at") <= F.lit(str(self._end_snapshot_cutoff)))
            .withColumn("committed_at", column_to_string_utc("committed_at"))
        )

    def _query_row_lineage_by_snapshot_id(self) -> Dict[int, SnapshotRowLineage]:
        try:
            return self._read_row_lineage_by_snapshot_id()

        except Exception as e:
            logger.error(f"[{self._table_name}] snapshot row lineage collection failed", exc_info=True)
            self._warnings["snapshot_row_lineage"] = [str(e)]

            return {}

    def _read_row_lineage_by_snapshot_id(self) -> Dict[int, SnapshotRowLineage]:
        metadata_df = self._spark.read.option("multiLine", True).json(collect_graph_metadata_file(self._table_name, None))
        snapshots_type = metadata_df.schema["snapshots"].dataType if "snapshots" in metadata_df.columns else None
        if not (isinstance(snapshots_type, ArrayType) and isinstance(snapshots_type.elementType, StructType)):
            return {}

        snapshot_fields = snapshots_type.elementType.fieldNames()
        if "first-row-id" not in snapshot_fields or "added-rows" not in snapshot_fields:
            return {}

        snapshots_df = metadata_df.select(F.explode("snapshots").alias("snapshot")).select(
            F.col("snapshot.snapshot-id").alias("snapshot_id"),
            F.col("snapshot.first-row-id").alias("first_row_id"),
            F.col("snapshot.added-rows").alias("added_rows"),
        )

        return {row.snapshot_id: SnapshotRowLineage(row.first_row_id, row.added_rows) for row in snapshots_df.collect()}

    @staticmethod
    def _validate_snapshot_count(snapshots_df: pyspark.sql.DataFrame) -> None:
        if snapshots_df.count() > Env.MAX_SNAPSHOTS_TO_COMPUTE:
            raise ValueError(f"Too many snapshots to compute. Maximum is {Env.MAX_SNAPSHOTS_TO_COMPUTE}.")

    def _parse_snapshot_row(self, snapshot, row_lineage_by_snapshot_id: Dict[int, SnapshotRowLineage]) -> SnapshotRecord:
        row_lineage = row_lineage_by_snapshot_id.get(snapshot.snapshot_id, SnapshotRowLineage(None, None))

        return SnapshotRecord(
            type=FileType.SNAPSHOT,
            file_path=snapshot.manifest_list,
            timestamp=str(snapshot.committed_at),
            snapshot_id=snapshot.snapshot_id,
            parent_id=snapshot.parent_id,
            operation=snapshot.operation,
            operation_description=snapshot.operation,
            action_link=None,
            summary=format_snapshot_summary(snapshot.summary or {}),
            first_row_id=row_lineage.first_row_id,
            added_rows=row_lineage.added_rows,
            child_files=[],
        )
