from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List

import pyspark

from base_classes.base_file import BaseFile
from base_classes.spark_table_action import SparkTableAction
from extractors.extractor import Extractor
from icegraph_logger import logger


@dataclass(frozen=True)
class FilesCollection:
    files: List[BaseFile] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)


class Collector(SparkTableAction, ABC):
    @abstractmethod
    def collect(self) -> FilesCollection:
        pass

    def _collect_rows_isolating_failures(self, extractor: Extractor) -> List[pyspark.sql.Row]:
        try:
            return extractor.extract_dataframe().collect()
        except Exception:
            logger.warning(
                f"[{self._table_name}] Collection failed, retrying without the files that cannot be read",
                exc_info=True,
            )
            extractor.isolate_failing_sources()

        return extractor.extract_dataframe().collect()
