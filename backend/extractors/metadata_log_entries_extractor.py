from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Set, Tuple

import polars as pl
from pyspark.sql import DataFrame, Row, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

from base_classes.spark_table_action import SparkTableAction
from constants import SNAPSHOT_NOT_IN_METADATA_LOG_REASON
from extractors.snapshot_ranges import SnapshotCommit, SnapshotLog, SnapshotLogEntry, SnapshotRange, SnapshotRangesBuilder
from icegraph_logger import logger

METADATA_SNAPSHOTS_LINEAGE_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("parent-snapshot-id", LongType()),
            StructField("timestamp-ms", LongType()),
        ]
    )
)
METADATA_REFS_SCHEMA = MapType(StringType(), StructType([StructField("snapshot-id", LongType()), StructField("type", StringType())]))
METADATA_LOG_SCHEMA = ArrayType(StructType([StructField("timestamp-ms", LongType()), StructField("metadata-file", StringType())]))
METADATA_SNAPSHOT_LOG_SCHEMA = ArrayType(StructType([StructField("timestamp-ms", LongType()), StructField("snapshot-id", LongType())]))
METADATA_LOG_ENTRIES_SCHEMA = pl.Schema({"file": pl.String(), "timestamp_ms": pl.Int64(), "snapshot_id": pl.Int64()})
BRANCH_REF_TYPE = "branch"
MAIN_BRANCH_NAME = "main"


@dataclass
class MetadataFile:
    path: str
    timestamp_ms: int


@dataclass
class CurrentMetadata:
    metadata_files: List[MetadataFile]
    snapshots: List[SnapshotCommit]
    snapshot_log: SnapshotLog
    side_branch_head_ids: Set[int]


class MetadataLogEntriesExtractor(SparkTableAction):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.missing_snapshot_reason = SNAPSHOT_NOT_IN_METADATA_LOG_REASON

    def extract_entries(self) -> pl.DataFrame:
        return self._metadata_log_entries

    @cached_property
    def _metadata_log_entries(self) -> pl.DataFrame:
        current_metadata_entry = self._find_current_metadata_entry()
        if current_metadata_entry is None:
            return pl.DataFrame(schema=METADATA_LOG_ENTRIES_SCHEMA)

        metadata_row = self._read_current_metadata_row(current_metadata_entry.file)
        if metadata_row is None:
            current_metadata_entries = [(current_metadata_entry.file, current_metadata_entry.timestamp_ms, current_metadata_entry.snapshot_id)]

            return pl.DataFrame(current_metadata_entries, schema=METADATA_LOG_ENTRIES_SCHEMA, orient="row")

        return self._build_metadata_log_entries(self._to_current_metadata(metadata_row))

    def _find_current_metadata_entry(self) -> Optional[Row]:
        return (
            self._spark.sql(
                f"SELECT file, unix_millis(timestamp) AS timestamp_ms, latest_snapshot_id AS snapshot_id FROM {self._table_name}.metadata_log_entries"
            )
            .orderBy(F.desc("timestamp_ms"), F.desc(F.length("file")), F.desc("file"))
            .first()
        )

    def _read_current_metadata_row(self, current_metadata_file: str) -> Optional[Row]:
        try:
            metadata_row = self._metadata_fields_df(current_metadata_file).first()
            if metadata_row is None or not metadata_row.is_parsed:
                raise ValueError(f"The current metadata file could not be parsed: {current_metadata_file}")

            return metadata_row

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the snapshots from the current metadata file", exc_info=True)
            self.missing_snapshot_reason = str(e)

            return None

    def _metadata_fields_df(self, metadata_file: str) -> DataFrame:
        metadata_text_df = self._spark.read.text(metadata_file, wholetext=True)
        parsed_metadata_df = metadata_text_df.select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))

        return parsed_metadata_df.select(
            F.col("metadata").isNotNull().alias("is_parsed"),
            F.lit(metadata_file).alias("file"),
            F.col("metadata")["last-updated-ms"].cast(LongType()).alias("last_updated_ms"),
            F.from_json(F.col("metadata")["metadata-log"], METADATA_LOG_SCHEMA).alias("metadata_log"),
            F.from_json(F.col("metadata")["snapshots"], METADATA_SNAPSHOTS_LINEAGE_SCHEMA).alias("snapshots"),
            F.from_json(F.col("metadata")["snapshot-log"], METADATA_SNAPSHOT_LOG_SCHEMA).alias("snapshot_log"),
            F.from_json(F.col("metadata")["refs"], METADATA_REFS_SCHEMA).alias("refs"),
        )

    def _to_current_metadata(self, metadata_row: Row) -> CurrentMetadata:
        return CurrentMetadata(
            metadata_files=self._list_metadata_files(metadata_row),
            snapshots=self._list_snapshots(metadata_row),
            snapshot_log=self._read_snapshot_log(metadata_row),
            side_branch_head_ids=self._find_side_branch_head_ids(metadata_row),
        )

    @staticmethod
    def _list_metadata_files(metadata_row: Row) -> List[MetadataFile]:
        metadata_files = [MetadataFile(entry["metadata-file"], entry["timestamp-ms"]) for entry in metadata_row.metadata_log or []]
        metadata_files.append(MetadataFile(metadata_row.file, metadata_row.last_updated_ms))

        return metadata_files

    @staticmethod
    def _list_snapshots(metadata_row: Row) -> List[SnapshotCommit]:
        return [
            SnapshotCommit(snapshot["snapshot-id"], snapshot["parent-snapshot-id"], snapshot["timestamp-ms"])
            for snapshot in metadata_row.snapshots or []
        ]

    @staticmethod
    def _read_snapshot_log(metadata_row: Row) -> SnapshotLog:
        return SnapshotLog([SnapshotLogEntry(entry["snapshot-id"], entry["timestamp-ms"]) for entry in metadata_row.snapshot_log or []])

    @staticmethod
    def _find_side_branch_head_ids(metadata_row: Row) -> Set[int]:
        return {
            ref["snapshot-id"]
            for ref_name, ref in (metadata_row.refs or {}).items()
            if ref["type"] == BRANCH_REF_TYPE and ref_name != MAIN_BRANCH_NAME
        }

    def _build_metadata_log_entries(self, current_metadata: CurrentMetadata) -> pl.DataFrame:
        snapshot_log = current_metadata.snapshot_log
        branch_ranges = SnapshotRangesBuilder(current_metadata.snapshots, snapshot_log.snapshot_ids, current_metadata.side_branch_head_ids).build()

        metadata_log_entries = []
        for metadata_file in current_metadata.metadata_files:
            metadata_log_entries.extend(self._metadata_log_entries_of(metadata_file, snapshot_log, branch_ranges))

        return pl.DataFrame(metadata_log_entries, schema=METADATA_LOG_ENTRIES_SCHEMA, orient="row")

    @staticmethod
    def _metadata_log_entries_of(
        metadata_file: MetadataFile, snapshot_log: SnapshotLog, branch_ranges: List[SnapshotRange]
    ) -> List[Tuple[str, int, Optional[int]]]:
        snapshot_ids = {snapshot_log.snapshot_id_at(metadata_file.timestamp_ms)}
        snapshot_ids.update(branch_range.snapshot_id for branch_range in branch_ranges if branch_range.covers(metadata_file.timestamp_ms))
        snapshot_ids.discard(None)
        if not snapshot_ids:
            return [(metadata_file.path, metadata_file.timestamp_ms, None)]

        return [(metadata_file.path, metadata_file.timestamp_ms, snapshot_id) for snapshot_id in snapshot_ids]
