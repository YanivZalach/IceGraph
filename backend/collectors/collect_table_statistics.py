from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType

from base_classes.base_file import BaseFile, HiddenFile
from base_classes.utils import timed
from collectors.collect_metadata import MetadataFileRecord
from collectors.collector import Collector, FilesCollection
from constants import TABLE_STATISTICS_ATTRIBUTION_WARNING, TABLE_STATISTICS_BEFORE_RANGE_ERROR, TABLE_STATISTICS_COLLECTION_ERROR, FileType
from icegraph_logger import logger

TABLE_STATISTICS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("snapshot-id", LongType()),
            StructField("statistics-path", StringType()),
            StructField("file-size-in-bytes", LongType()),
            StructField("file-footer-size-in-bytes", LongType()),
            StructField("key-metadata", StringType()),
            StructField(
                "blob-metadata",
                ArrayType(
                    StructType(
                        [
                            StructField("type", StringType()),
                            StructField("snapshot-id", LongType()),
                            StructField("sequence-number", LongType()),
                            StructField("fields", ArrayType(IntegerType())),
                            StructField("properties", MapType(StringType(), StringType())),
                        ]
                    )
                ),
            ),
        ]
    )
)

METADATA_FILE_STATISTICS_SCHEMA = StructType([StructField("statistics", TABLE_STATISTICS_SCHEMA)])


@dataclass
class HiddenTableStatisticsMetadata(HiddenFile):
    added_by_metadata_file: str


@dataclass
class TableStatisticsFileRecord(BaseFile):
    snapshot_id: int
    file_size_in_bytes: str
    file_footer_size_in_bytes: str
    key_metadata: Optional[str]
    blobs: List[Dict[str, Any]]
    hidden_table_statistics_data: HiddenTableStatisticsMetadata


class CollectTableStatistics(Collector):
    def __init__(
        self,
        full_table_name: str,
        metadata_files: List[MetadataFileRecord],
    ):
        super().__init__(full_table_name)
        self._metadata_files = metadata_files

        self._table_statistics_files: List[TableStatisticsFileRecord] = []
        self._errors: Dict[str, List[str]] = {}
        self._warnings: Dict[str, List[str]] = {}

    @timed
    def collect(self) -> FilesCollection:
        if not any(metadata_file.pointed_statistics_files for metadata_file in self._metadata_files):
            return FilesCollection()

        if any(metadata_file.errors for metadata_file in self._metadata_files):
            self._warnings["table_statistics_attribution"] = [TABLE_STATISTICS_ATTRIBUTION_WARNING]

        statistics_paths_before_range = self._find_statistics_paths_before_range()
        metadata_file_to_added_paths = self._find_added_statistics_paths(statistics_paths_before_range)
        if not metadata_file_to_added_paths:
            return FilesCollection(errors=self._errors, warnings=self._warnings)

        metadata_file_to_statistics_entries = self._collect_statistics_entries(metadata_file_to_added_paths)
        if metadata_file_to_statistics_entries is None:
            return FilesCollection(errors=self._errors, warnings=self._warnings)

        self._collect_statistics_files(metadata_file_to_statistics_entries)

        return FilesCollection(files=self._table_statistics_files, errors=self._errors, warnings=self._warnings)

    def _collect_statistics_entries(self, metadata_file_to_added_paths: Dict[str, Set[str]]) -> Optional[Dict[str, List[dict]]]:
        try:
            rows = self._read_table_statistics(list(metadata_file_to_added_paths.keys()), metadata_file_to_added_paths).collect()
        except Exception as e:
            logger.error(f"[{self._table_name}] Table statistics read error", exc_info=True)
            self._errors["table_statistics_collection"] = [TABLE_STATISTICS_COLLECTION_ERROR.format(error=e)]
            return None

        return {row.file: row.asDict(recursive=True)["statistics"] or [] for row in rows}

    def _collect_statistics_files(self, metadata_file_to_statistics_entries: Dict[str, List[dict]]) -> None:
        for metadata_file in self._metadata_files:
            entries = metadata_file_to_statistics_entries.get(metadata_file.file_path, [])
            for entry in sorted(entries, key=lambda entry: entry["statistics-path"]):
                self._add_table_statistics_file(metadata_file, entry)

    def _find_statistics_paths_before_range(self) -> Set[str]:
        metadata_file_before_range = self._find_metadata_file_before_range()
        if metadata_file_before_range is None:
            return set()

        try:
            rows = self._read_table_statistics([metadata_file_before_range]).collect()
        except Exception:
            logger.error(f"[{self._table_name}] Metadata file before range read error for {metadata_file_before_range}", exc_info=True)
            self._errors["table_statistics_before_range"] = [TABLE_STATISTICS_BEFORE_RANGE_ERROR.format(metadata_file=metadata_file_before_range)]
            return set()

        return {entry["statistics-path"] for row in rows for entry in row.statistics or [] if entry["statistics-path"] is not None}

    def _find_metadata_file_before_range(self) -> Optional[str]:
        log_df = self._spark.sql(f"SELECT file, timestamp FROM {self._table_name}.metadata_log_entries")
        oldest_collected_df = log_df.filter(
            F.col("file") == F.lit(self._metadata_files[-1].file_path)  # ordered list
        ).select(F.col("timestamp").alias("oldest_collected_timestamp"))
        row = (
            log_df.crossJoin(oldest_collected_df)
            .filter(F.col("timestamp") < F.col("oldest_collected_timestamp"))
            .orderBy(F.desc("timestamp"))
            .select("file")
            .first()
        )

        return row.file if row else None

    def _read_table_statistics(
        self,
        metadata_files: List[str],
        metadata_file_to_added_paths: Optional[Dict[str, Set[str]]] = None,
    ) -> pyspark.sql.DataFrame:
        statistics_df = None
        for metadata_file in metadata_files:
            df = (
                self._spark.read.schema(METADATA_FILE_STATISTICS_SCHEMA)
                .option("multiLine", True)
                .json(metadata_file)
                .select(F.lit(metadata_file).alias("file"), "statistics")
            )
            if metadata_file_to_added_paths is not None:
                added_paths = sorted(metadata_file_to_added_paths[metadata_file])
                df = df.withColumn("statistics", F.filter("statistics", lambda entry: entry["statistics-path"].isin(added_paths)))

            statistics_df = df if statistics_df is None else statistics_df.unionByName(df)

        return statistics_df

    def _find_added_statistics_paths(self, statistics_paths_to_ignore: Set[str]) -> Dict[str, Set[str]]:
        seen_paths = set(statistics_paths_to_ignore)
        metadata_file_to_added_paths: Dict[str, Set[str]] = {}

        for metadata_file in reversed(self._metadata_files):
            for statistics_path in (metadata_file.pointed_statistics_files or {}).values():
                if statistics_path in seen_paths:
                    continue

                seen_paths.add(statistics_path)
                metadata_file_to_added_paths.setdefault(metadata_file.file_path, set()).add(statistics_path)

        return metadata_file_to_added_paths

    def _add_table_statistics_file(self, metadata_file: MetadataFileRecord, entry: dict) -> None:
        try:
            table_statistics_file = self._parse_table_statistics_entry(entry, metadata_file.file_path)
        except Exception as e:
            msg = f"Failed to read table statistics entry {entry['statistics-path']}: {type(e).__name__}: {e}"
            logger.error(f"[{self._table_name}] {msg} in {metadata_file.file_path}", exc_info=True)
            metadata_file.errors.append(msg)
            return

        self._table_statistics_files.append(table_statistics_file)

    @staticmethod
    def _parse_table_statistics_entry(entry: dict, added_by_metadata_file: str) -> TableStatisticsFileRecord:
        return TableStatisticsFileRecord(
            type=FileType.TABLE_STATISTICS,
            virtual_read=True,
            file_path=entry["statistics-path"],
            child_files=[],
            snapshot_id=entry["snapshot-id"],
            file_size_in_bytes=str(entry["file-size-in-bytes"]),
            file_footer_size_in_bytes=str(entry["file-footer-size-in-bytes"]),
            key_metadata=entry["key-metadata"],
            blobs=[
                {
                    "type": blob["type"],
                    "fields": blob["fields"],
                    "snapshot_id": blob["snapshot-id"],
                    "sequence_number": blob["sequence-number"],
                    "properties": blob["properties"] or {},
                }
                for blob in entry["blob-metadata"]
            ],
            hidden_table_statistics_data=HiddenTableStatisticsMetadata(added_by_metadata_file=added_by_metadata_file),
        )
