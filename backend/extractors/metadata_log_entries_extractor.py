from dataclasses import dataclass
from functools import cached_property
from typing import List, Optional, Set, Tuple

import polars as pl
from pyspark.sql import DataFrame, Row, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType, StructField, StructType

from base_classes.spark_table_action import SparkTableAction
from constants import SNAPSHOT_NOT_IN_METADATA_LOG_REASON
from extractors.snapshot_ranges import SnapshotCommit, SnapshotRange, SnapshotRangesBuilder
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
METADATA_LOG_ENTRIES_SCHEMA = pl.Schema({"file": pl.String(), "timestamp_ms": pl.Int64(), "snapshot_id": pl.Int64()})
BRANCH_REF_TYPE = "branch"


@dataclass
class MetadataFile:
    path: str
    timestamp_ms: int


@dataclass
class CurrentMetadata:
    metadata_files: List[MetadataFile]
    snapshots: List[SnapshotCommit]
    possible_live_tip_ids: Set[int]


class MetadataLogEntriesExtractor(SparkTableAction):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.missing_snapshot_reason = SNAPSHOT_NOT_IN_METADATA_LOG_REASON

    def extract_entries(self) -> pl.DataFrame:
        return self._metadata_log_entries

    @staticmethod
    def find_newest_file(metadata_log_entries: pl.DataFrame) -> Optional[str]:
        newest_entries = metadata_log_entries.sort(
            [pl.col("timestamp_ms"), pl.col("file").str.len_chars(), pl.col("file")],
            descending=True,
        )
        if newest_entries.is_empty():
            return None

        return newest_entries["file"][0]

    @cached_property
    def _metadata_log_entries(self) -> pl.DataFrame:
        metadata_log_view_entries = self._collect_metadata_log_view()
        metadata_row = self._read_current_metadata_row(metadata_log_view_entries)
        if metadata_row is None:
            return metadata_log_view_entries

        return self._build_metadata_log_entries(self._to_current_metadata(metadata_row))

    def _collect_metadata_log_view(self) -> pl.DataFrame:
        metadata_log_view_rows = self._spark.sql(
            f"SELECT file, unix_millis(timestamp) AS timestamp_ms, latest_snapshot_id AS snapshot_id FROM {self._table_name}.metadata_log_entries"
        ).collect()
        metadata_log_view_entries = [(row.file, row.timestamp_ms, row.snapshot_id) for row in metadata_log_view_rows]

        return pl.DataFrame(metadata_log_view_entries, schema=METADATA_LOG_ENTRIES_SCHEMA, orient="row")

    def _read_current_metadata_row(self, metadata_log_view_entries: pl.DataFrame) -> Optional[Row]:
        current_metadata_file = self.find_newest_file(metadata_log_view_entries)
        if current_metadata_file is None:
            return None

        try:
            return self._read_metadata_row(current_metadata_file)

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the snapshots from the current metadata file", exc_info=True)
            self.missing_snapshot_reason = str(e)

            return None

    def _read_metadata_row(self, metadata_file: str) -> Row:
        metadata_row = self._metadata_fields_df(metadata_file).first()
        if metadata_row is None or not metadata_row.is_parsed:
            raise ValueError(f"The current metadata file could not be parsed: {metadata_file}")

        return metadata_row

    def _metadata_fields_df(self, metadata_file: str) -> DataFrame:
        metadata_text_df = self._spark.read.text(metadata_file, wholetext=True)
        parsed_metadata_df = metadata_text_df.select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))

        return parsed_metadata_df.select(
            F.col("metadata").isNotNull().alias("is_parsed"),
            F.lit(metadata_file).alias("file"),
            F.col("metadata")["last-updated-ms"].cast(LongType()).alias("last_updated_ms"),
            F.col("metadata")["current-snapshot-id"].cast(LongType()).alias("current_snapshot_id"),
            F.from_json(F.col("metadata")["metadata-log"], METADATA_LOG_SCHEMA).alias("metadata_log"),
            F.from_json(F.col("metadata")["snapshots"], METADATA_SNAPSHOTS_LINEAGE_SCHEMA).alias("snapshots"),
            F.from_json(F.col("metadata")["refs"], METADATA_REFS_SCHEMA).alias("refs"),
        )

    def _to_current_metadata(self, metadata_row: Row) -> CurrentMetadata:
        return CurrentMetadata(
            metadata_files=self._list_metadata_files(metadata_row),
            snapshots=self._list_snapshots(metadata_row),
            possible_live_tip_ids=self._find_possible_live_tip_ids(metadata_row),
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
    def _find_possible_live_tip_ids(metadata_row: Row) -> Set[int]:
        branch_head_ids = {ref["snapshot-id"] for ref in (metadata_row.refs or {}).values() if ref["type"] == BRANCH_REF_TYPE}

        return branch_head_ids | {metadata_row.current_snapshot_id}

    def _build_metadata_log_entries(self, current_metadata: CurrentMetadata) -> pl.DataFrame:
        snapshot_ranges = SnapshotRangesBuilder(current_metadata.snapshots, current_metadata.possible_live_tip_ids).build()

        metadata_log_entries = []
        for metadata_file in current_metadata.metadata_files:
            metadata_log_entries.extend(self._metadata_log_entries_of(metadata_file, snapshot_ranges))

        return pl.DataFrame(metadata_log_entries, schema=METADATA_LOG_ENTRIES_SCHEMA, orient="row")

    @staticmethod
    def _metadata_log_entries_of(metadata_file: MetadataFile, snapshot_ranges: List[SnapshotRange]) -> List[Tuple[str, int, Optional[int]]]:
        covering_snapshot_ids = [
            snapshot_range.snapshot_id for snapshot_range in snapshot_ranges if snapshot_range.covers(metadata_file.timestamp_ms)
        ]
        if not covering_snapshot_ids:
            return [(metadata_file.path, metadata_file.timestamp_ms, None)]

        return [(metadata_file.path, metadata_file.timestamp_ms, snapshot_id) for snapshot_id in covering_snapshot_ids]
