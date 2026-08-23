from dataclasses import dataclass
from typing import Dict, List, Optional

import pyspark.sql
from arrow import Arrow
from pyspark.sql import functions as F

from base_classes.base_file import BaseFile
from base_classes.utils import column_to_string_utc, timed
from collectors.collector import Collector, FilesCollection
from collectors.utils import bytes_to_gib
from constants import FileType, SnapshotSummary
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
    summary: SnapshotSummary


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
        self._errors: Dict[str, str] = {}

    @timed
    def collect(self) -> FilesCollection:
        try:
            snapshots_df = self._query_snapshots_df()
            self._validate_snapshot_count(snapshots_df)
            self._snapshots = [self._parse_snapshot_row(row) for row in snapshots_df.collect()]
        except Exception as e:
            logger.error(f"[{self._table_name}] snapshots collection failed", exc_info=True)
            self._errors["snapshot_collection"] = str(e)

        return FilesCollection(files=self._snapshots, errors=self._errors)

    def _query_snapshots_df(self) -> pyspark.sql.DataFrame:
        return (
            self._spark.sql(f"SELECT * FROM {self._table_name}.snapshots ORDER BY committed_at DESC")
            .filter(F.col("committed_at") >= F.lit(str(self._start_snapshot_cutoff)))
            .filter(F.col("committed_at") <= F.lit(str(self._end_snapshot_cutoff)))
            .withColumn("committed_at", column_to_string_utc("committed_at"))
        )

    @staticmethod
    def _validate_snapshot_count(snapshots_df: pyspark.sql.DataFrame) -> None:
        if snapshots_df.count() > Env.MAX_SNAPSHOTS_TO_COMPUTE:
            raise ValueError(f"Too many snapshots to compute. Maximum is {Env.MAX_SNAPSHOTS_TO_COMPUTE}.")

    @staticmethod
    def _format_summary(summary: Dict[str, str]) -> SnapshotSummary:
        formatted = {}
        for key, value in summary.items():
            if key.endswith("files-size"):
                try:
                    formatted[f"{key}-gib"] = bytes_to_gib(int(value))
                except Exception:
                    formatted[key] = value

            else:
                formatted[key] = value

        return formatted

    def _parse_snapshot_row(self, snapshot) -> SnapshotRecord:
        return SnapshotRecord(
            type=FileType.SNAPSHOT,
            file_path=snapshot.manifest_list,
            timestamp=str(snapshot.committed_at),
            snapshot_id=snapshot.snapshot_id,
            parent_id=snapshot.parent_id,
            operation=snapshot.operation,
            operation_description=snapshot.operation,
            action_link=None,
            summary=self._format_summary(snapshot.summary or {}),
            child_files=[],
        )
