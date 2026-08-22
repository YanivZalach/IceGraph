from abc import ABC, abstractmethod
from typing import Dict, List

from pydantic import BaseModel, ConfigDict, Field

from base_classes.base_file import BaseFile
from base_classes.spark_table_action import SparkTableAction


class FilesCollection(BaseModel):
    model_config = ConfigDict(frozen=True)

    files: List[BaseFile] = Field(default_factory=list)
    errors: Dict[str, str] = Field(default_factory=dict)


class Collector(SparkTableAction, ABC):
    @abstractmethod
    def collect(self) -> FilesCollection:
        pass
