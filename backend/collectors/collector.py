import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List

import pyspark

from base_classes.base_file import BaseFile
from base_classes.spark_table_action import SparkTableAction
from extractors.extractor import Extractor

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FilesCollection:
    files: List[BaseFile] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)
    warnings: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class RowsCollection:
    rows: List[pyspark.sql.Row] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)


class Collector(SparkTableAction, ABC):
    @abstractmethod
    def collect(self) -> FilesCollection:
        pass

    def _collect_rows_isolating_failures(self, extractor: Extractor) -> RowsCollection:
        extraction_result = extractor.extract_dataframe()

        try:
            return RowsCollection(rows=extraction_result.dataframe.collect(), errors=extraction_result.errors)
        except Exception:
            logger.warning(
                f"[{self._table_name}] Collection failed, retrying without the files that cannot be read",
                exc_info=True,
            )
            if not extractor.isolate_failing_sources():
                raise

        retry_result = extractor.extract_dataframe()

        return RowsCollection(rows=retry_result.dataframe.collect(), errors=retry_result.errors)
