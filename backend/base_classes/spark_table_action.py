from abc import ABC
from functools import cached_property

from spark_connect import open_spark_connect_session


class SparkTableAction(ABC):
    def __init__(
        self,
        full_table_name: str,
    ):
        self._table_name = full_table_name

    @cached_property
    def _spark(self):
        return open_spark_connect_session()
