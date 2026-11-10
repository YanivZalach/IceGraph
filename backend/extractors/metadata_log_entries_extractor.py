from functools import cached_property
from typing import List, Optional, Set, Tuple

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import LongType, MapType, StringType

from constants import SNAPSHOT_NOT_IN_METADATA_LOG_REASON
from extractors.extractor import Extractor
from extractors.utils import (
    BRANCH_REF_TYPE,
    METADATA_LOG_ENTRIES_SCHEMA,
    METADATA_LOG_SCHEMA,
    METADATA_REFS_SCHEMA,
    METADATA_SNAPSHOTS_LINEAGE_SCHEMA,
    SnapshotRange,
    build_snapshot_ranges,
)
from icegraph_logger import logger


class MetadataLogEntriesExtractor(Extractor):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.missing_snapshot_reason = SNAPSHOT_NOT_IN_METADATA_LOG_REASON

    def extract_dataframe(self) -> DataFrame:
        return self._metadata_log_entries_df

    @cached_property
    def _metadata_log_entries_df(self) -> DataFrame:
        metadata_log_df = self._spark.sql(f"SELECT timestamp, file, latest_snapshot_id FROM {self._table_name}.metadata_log_entries")
        metadata = self._read_current_metadata(metadata_log_df)
        if metadata is None:
            return metadata_log_df.withColumnRenamed("latest_snapshot_id", "snapshot_id")

        snapshot_ranges = build_snapshot_ranges(metadata["snapshots"] or [], self._find_live_tip_ids(metadata))
        metadata_log_entries = self._build_metadata_log_entries(metadata, snapshot_ranges)

        return self._spark.createDataFrame(metadata_log_entries, METADATA_LOG_ENTRIES_SCHEMA).withColumn(
            "timestamp", F.timestamp_millis("timestamp_ms")
        )

    def _read_current_metadata(self, metadata_log_df: DataFrame) -> Optional[dict]:
        current_metadata_file = metadata_log_df.orderBy(F.desc("timestamp"), F.desc(F.length("file")), F.desc("file")).select("file").first()
        if current_metadata_file is None:
            return None

        try:
            return self._read_metadata(current_metadata_file.file)

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the snapshots from the current metadata file", exc_info=True)
            self.missing_snapshot_reason = str(e)

            return None

    def _read_metadata(self, metadata_file: str) -> dict:
        metadata = F.col("metadata")
        row = (
            self._spark.read.text(metadata_file, wholetext=True)
            .select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))
            .select(
                metadata.isNotNull().alias("is_parsed"),
                F.lit(metadata_file).alias("file"),
                metadata.getItem("last-updated-ms").cast(LongType()).alias("last_updated_ms"),
                metadata.getItem("current-snapshot-id").cast(LongType()).alias("current_snapshot_id"),
                F.from_json(metadata.getItem("metadata-log"), METADATA_LOG_SCHEMA).alias("metadata_log"),
                F.from_json(metadata.getItem("snapshots"), METADATA_SNAPSHOTS_LINEAGE_SCHEMA).alias("snapshots"),
                F.from_json(metadata.getItem("refs"), METADATA_REFS_SCHEMA).alias("refs"),
            )
            .first()
        )
        if row is None or not row.is_parsed:
            raise ValueError(f"The current metadata file could not be parsed: {metadata_file}")

        return row.asDict(recursive=True)

    @staticmethod
    def _find_live_tip_ids(metadata: dict) -> Set[int]:
        branch_head_ids = {ref["snapshot-id"] for ref in (metadata["refs"] or {}).values() if ref["type"] == BRANCH_REF_TYPE}

        return branch_head_ids | {metadata["current_snapshot_id"]}

    @staticmethod
    def _build_metadata_log_entries(metadata: dict, snapshot_ranges: List[SnapshotRange]) -> List[Tuple[str, int, Optional[int]]]:
        metadata_files = [(entry["metadata-file"], entry["timestamp-ms"]) for entry in metadata["metadata_log"] or []]
        metadata_files.append((metadata["file"], metadata["last_updated_ms"]))

        metadata_log_entries = []
        for file, timestamp_ms in metadata_files:
            covering_snapshot_ids = [snapshot_range.snapshot_id for snapshot_range in snapshot_ranges if snapshot_range.covers(timestamp_ms)]
            for snapshot_id in covering_snapshot_ids or [None]:
                metadata_log_entries.append((file, timestamp_ms, snapshot_id))

        return metadata_log_entries
