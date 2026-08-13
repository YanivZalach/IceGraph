from abc import ABC, abstractmethod
from typing import Callable, Optional

import pyspark

from base_classes.base_file import BaseFile
from base_classes.spark_table_action import SparkTableAction
from icegraph_logger import logger


class Extractor(SparkTableAction, ABC):
    @abstractmethod
    def extract_dataframe(self) -> pyspark.sql.DataFrame:
        pass

    def _read_source(self, source_file: BaseFile, read_source: Callable[[], pyspark.sql.DataFrame]) -> Optional[pyspark.sql.DataFrame]:
        try:
            data = read_source()
            data.schema  # Trigger the file metadata read

            return data

        except Exception as e:
            logger.error(f"[{self._table_name}] Failed to read file {source_file.file_path}", exc_info=True)
            source_file.error = str(e)

            return None
