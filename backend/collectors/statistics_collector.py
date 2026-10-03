from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

import pyspark.sql
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from base_classes.base_file import BaseFile, HiddenFile
from base_classes.utils import timed
from collectors.collect_metadata import MetadataFileRecord
from collectors.collector import Collector, FilesCollection
from constants import STATISTICS_ATTRIBUTION_WARNING, STATISTICS_BEFORE_RANGE_ERROR
from icegraph_logger import logger


@dataclass
class HiddenStatisticsMetadata(HiddenFile):
    added_by_metadata_file: str


class StatisticsCollector(Collector, ABC):
    STATISTICS_COLUMN: str
    STATISTICS_KEY: str
    STATISTICS_NAME: str

    def __init__(
        self,
        full_table_name: str,
        metadata_files: List[MetadataFileRecord],
    ):
        super().__init__(full_table_name)
        self._metadata_files = metadata_files

        self._statistics_files: List[BaseFile] = []
        self._errors: Dict[str, List[str]] = {}
        self._warnings: Dict[str, List[str]] = {}

    @timed
    def collect(self) -> FilesCollection:
        if not any(self._get_pointed_statistics(metadata_file) for metadata_file in self._metadata_files):
            return FilesCollection()

        if any(self._get_pointed_statistics(metadata_file) is None for metadata_file in self._metadata_files):
            self._warnings[f"{self.STATISTICS_KEY}_attribution"] = [STATISTICS_ATTRIBUTION_WARNING.format(statistics_name=self.STATISTICS_NAME)]

        statistics_paths_before_range = self._find_statistics_paths_before_range()
        metadata_file_to_added_entries = self._find_added_statistics_entries(statistics_paths_before_range)
        if metadata_file_to_added_entries:
            self._collect_statistics_files(metadata_file_to_added_entries)

        return FilesCollection(files=self._statistics_files, errors=self._errors, warnings=self._warnings)

    @abstractmethod
    def _get_pointed_statistics(self, metadata_file: MetadataFileRecord) -> Optional[List[dict]]:
        pass

    @abstractmethod
    def _collect_statistics_files(self, metadata_file_to_added_entries: Dict[str, List[dict]]) -> None:
        pass

    def _find_statistics_paths_before_range(self) -> Set[str]:
        metadata_file_before_range = self._find_metadata_file_before_range()
        if metadata_file_before_range is None:
            return set()

        statistics_paths_schema = StructType(
            [StructField(self.STATISTICS_COLUMN, ArrayType(StructType([StructField("statistics-path", StringType())])))]
        )
        try:
            rows = self._read_metadata_statistics([metadata_file_before_range], statistics_paths_schema).collect()
        except Exception:
            logger.error(f"[{self._table_name}] Metadata file before range read error for {metadata_file_before_range}", exc_info=True)
            self._errors[f"{self.STATISTICS_KEY}_before_range"] = [
                STATISTICS_BEFORE_RANGE_ERROR.format(metadata_file=metadata_file_before_range, statistics_name=self.STATISTICS_NAME)
            ]
            return set()

        return {entry["statistics-path"] for row in rows for entry in row[self.STATISTICS_COLUMN] or [] if entry["statistics-path"] is not None}

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

    def _read_metadata_statistics(
        self,
        metadata_files: List[str],
        schema: StructType,
        metadata_file_to_added_paths: Optional[Dict[str, Set[str]]] = None,
    ) -> pyspark.sql.DataFrame:
        statistics_df = None
        for metadata_file in metadata_files:
            df = (
                self._spark.read.schema(schema)
                .option("multiLine", True)
                .json(metadata_file)
                .select(F.lit(metadata_file).alias("file"), self.STATISTICS_COLUMN)
            )
            if metadata_file_to_added_paths is not None:
                added_paths = sorted(metadata_file_to_added_paths[metadata_file])
                df = df.withColumn(
                    self.STATISTICS_COLUMN,
                    F.filter(self.STATISTICS_COLUMN, lambda entry: entry["statistics-path"].isin(added_paths)),
                )

            statistics_df = df if statistics_df is None else statistics_df.unionByName(df)

        return statistics_df

    def _find_added_statistics_entries(self, statistics_paths_to_ignore: Set[str]) -> Dict[str, List[dict]]:
        seen_paths = set(statistics_paths_to_ignore)
        metadata_file_to_added_entries: Dict[str, List[dict]] = {}

        for metadata_file in reversed(self._metadata_files):
            for entry in self._get_pointed_statistics(metadata_file) or []:
                if entry["statistics-path"] in seen_paths:
                    continue

                seen_paths.add(entry["statistics-path"])
                metadata_file_to_added_entries.setdefault(metadata_file.file_path, []).append(entry)

        return metadata_file_to_added_entries
