from abc import ABC, abstractmethod
from typing import Callable, Optional

import pyspark

from base_classes.base_file import BaseFile
from base_classes.spark_table_action import SparkTableAction
from extractors.constants import DEFAULT_SOURCE_ERROR_PREFIX
from icegraph_logger import logger


class Extractor(SparkTableAction, ABC):
    SOURCE_ERROR_PREFIX = DEFAULT_SOURCE_ERROR_PREFIX

    @abstractmethod
    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        pass

    def _read_source(self, source_file: BaseFile, read_source: Callable[[], pyspark.sql.DataFrame]) -> Optional[pyspark.sql.DataFrame]:
        try:
            source_dataframe = read_source()
            source_dataframe.count()

            return source_dataframe

        except Exception as e:
            logger.warning(f"[{self._table_name}] Skipping unreadable file {source_file.file_path}", exc_info=True)
            source_file.error = f"{self.SOURCE_ERROR_PREFIX}: {e}"

            return None
