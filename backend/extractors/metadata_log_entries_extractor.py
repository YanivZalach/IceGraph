from functools import cached_property
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, Row, functions as F
from pyspark.sql.types import ArrayType, LongType, MapType, StringType

from constants import NO_REF_LEADS_TO_SNAPSHOT_REASON
from extractors.extractor import Extractor
from extractors.utils import (
    MAIN_BRANCH,
    METADATA_LOG_ENTRIES_SCHEMA,
    METADATA_LOG_SCHEMA,
    METADATA_REFS_SCHEMA,
    METADATA_SNAPSHOT_LOG_SCHEMA,
    METADATA_SNAPSHOTS_LINEAGE_SCHEMA,
    HeadChange,
    ancestry_timeline,
    head_at,
    heads_at,
)
from icegraph_logger import logger


class MetadataLogEntriesExtractor(Extractor):
    def __init__(self, table_name: str):
        super().__init__(table_name)
        self.missing_snapshot_reason = NO_REF_LEADS_TO_SNAPSHOT_REASON

    def extract_dataframe(self) -> DataFrame:
        return self._metadata_log_entries_df

    @cached_property
    def _metadata_log_entries_df(self) -> DataFrame:
        metadata_log_df = self._spark.sql(f"SELECT timestamp, file, latest_snapshot_id FROM {self._table_name}.metadata_log_entries")
        current_metadata_file = metadata_log_df.orderBy(F.desc("timestamp"), F.desc("file")).select("file").first()
        if current_metadata_file is None:
            return self._without_branch_snapshot_ids(metadata_log_df)

        try:
            metadata = self._read_metadata(current_metadata_file.file)

        except Exception as e:
            logger.warning(f"[{self._table_name}] Failed to read the branches and tags from the current metadata file", exc_info=True)
            self.missing_snapshot_reason = str(e)

            return self._without_branch_snapshot_ids(metadata_log_df)

        metadata_log_entries = self._build_metadata_log_entries(current_metadata_file.file, metadata)

        return self._spark.createDataFrame(metadata_log_entries, METADATA_LOG_ENTRIES_SCHEMA).withColumn(
            "timestamp", F.timestamp_millis("timestamp_ms")
        )

    def _read_metadata(self, metadata_file: str) -> Row:
        metadata = F.col("metadata")
        row = (
            self._spark.read.text(metadata_file, wholetext=True)
            .select(F.from_json("value", MapType(StringType(), StringType())).alias("metadata"))
            .select(
                metadata.isNotNull().alias("is_parsed"),
                metadata.getItem("last-updated-ms").cast(LongType()).alias("last_updated_ms"),
                F.from_json(metadata.getItem("metadata-log"), METADATA_LOG_SCHEMA).alias("metadata_log"),
                F.from_json(metadata.getItem("snapshot-log"), METADATA_SNAPSHOT_LOG_SCHEMA).alias("snapshot_log"),
                F.from_json(metadata.getItem("snapshots"), METADATA_SNAPSHOTS_LINEAGE_SCHEMA).alias("snapshots"),
                F.from_json(metadata.getItem("refs"), METADATA_REFS_SCHEMA).alias("refs"),
            )
            .first()
        )
        if row is None or not row.is_parsed:
            raise ValueError(f"The current metadata file could not be parsed: {metadata_file}")

        return row

    @staticmethod
    def _build_metadata_log_entries(current_metadata_file: str, metadata: Row) -> List[Tuple[str, int, Optional[int], List[int]]]:
        metadata_files = [(entry["metadata-file"], entry["timestamp-ms"]) for entry in metadata.metadata_log or []]
        metadata_files.append((current_metadata_file, metadata.last_updated_ms))

        main_timeline = [HeadChange(entry["timestamp-ms"], entry["snapshot-id"]) for entry in metadata.snapshot_log or []]
        snapshots_by_id = {snapshot["snapshot-id"]: snapshot for snapshot in metadata.snapshots or []}
        ref_timelines = [
            ancestry_timeline(ref["snapshot-id"], snapshots_by_id) for ref_name, ref in (metadata.refs or {}).items() if ref_name != MAIN_BRANCH
        ]
        main_snapshot_ids = {head_at(main_timeline, timestamp_ms) for _, timestamp_ms in metadata_files}

        return [
            (file, timestamp_ms, head_at(main_timeline, timestamp_ms), sorted(heads_at(ref_timelines, timestamp_ms) - main_snapshot_ids))
            for file, timestamp_ms in metadata_files
        ]

    @staticmethod
    def _without_branch_snapshot_ids(metadata_log_df: DataFrame) -> DataFrame:
        return metadata_log_df.withColumn("branch_snapshot_ids", F.array().cast(ArrayType(LongType())))
