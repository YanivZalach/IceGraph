from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Set

import pyspark

from base_classes.spark_table_action import SparkTableAction
from extractors.constants import DEFAULT_SOURCE_ERROR_PREFIX


@dataclass(frozen=True)
class ExtractionResult:
    dataframe: pyspark.sql.DataFrame
    errors: Dict[str, str] = field(default_factory=dict)


class Extractor(SparkTableAction, ABC):
    SOURCE_ERROR_PREFIX = DEFAULT_SOURCE_ERROR_PREFIX

    def __init__(self, full_table_name: str):
        super().__init__(full_table_name)
        self._errors: Dict[str, str] = {}
        self._source_dataframes: Dict[str, pyspark.sql.DataFrame] = {}
        self._failed_source_paths: Set[str] = set()

    @abstractmethod
    def extract_dataframe(self) -> ExtractionResult:
        pass

    def isolate_failing_sources(self) -> bool:
        found_failing_source = False

        for source_path, source_dataframe in list(self._source_dataframes.items()):
            try:
                source_dataframe.collect()
            except Exception as e:
                self._record_source_failure(source_path, e)
                self._source_dataframes.pop(source_path)
                found_failing_source = True

        return found_failing_source

    def _read_source(self, source_path: str, read_source: Callable[[], pyspark.sql.DataFrame]) -> Optional[pyspark.sql.DataFrame]:
        if source_path in self._failed_source_paths:
            return None

        try:
            source_dataframe = read_source()
        except Exception as e:
            self._record_source_failure(source_path, e)
            return None

        self._source_dataframes[source_path] = source_dataframe

        return source_dataframe

    def _record_source_failure(self, source_path: str, error: Exception) -> None:
        self._errors[source_path] = f"{self.SOURCE_ERROR_PREFIX}: {error}"
        self._failed_source_paths.add(source_path)
