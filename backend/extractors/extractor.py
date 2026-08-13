from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Dict, Optional, Set

import pyspark

from base_classes.base_file import BaseFile
from base_classes.spark_table_action import SparkTableAction
from extractors.constants import DEFAULT_SOURCE_ERROR_PREFIX


@dataclass(frozen=True)
class SourceRead:
    file: BaseFile
    dataframe: pyspark.sql.DataFrame


class Extractor(SparkTableAction, ABC):
    SOURCE_ERROR_PREFIX = DEFAULT_SOURCE_ERROR_PREFIX

    def __init__(self, full_table_name: str):
        super().__init__(full_table_name)
        self._sources: Dict[str, SourceRead] = {}
        self._failed_source_paths: Set[str] = set()

    @abstractmethod
    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        pass

    def isolate_failing_sources(self) -> None:
        for source_path, source in list(self._sources.items()):
            try:
                source.dataframe.collect()
            except Exception as e:
                self._record_source_failure(source.file, e)
                self._sources.pop(source_path)

    def _read_source(self, source_file: BaseFile, read_source: Callable[[], pyspark.sql.DataFrame]) -> Optional[pyspark.sql.DataFrame]:
        if source_file.file_path in self._failed_source_paths:
            return None

        try:
            source_dataframe = read_source()
        except Exception as e:
            self._record_source_failure(source_file, e)
            return None

        self._sources[source_file.file_path] = SourceRead(file=source_file, dataframe=source_dataframe)

        return source_dataframe

    def _record_source_failure(self, source_file: BaseFile, error: Exception) -> None:
        source_file.error = f"{self.SOURCE_ERROR_PREFIX}: {error}"
        self._failed_source_paths.add(source_file.file_path)
