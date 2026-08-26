from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List

from base_classes.base_file import BaseFile
from base_classes.spark_table_action import SparkTableAction


@dataclass(frozen=True)
class FilesCollection:
    files: List[BaseFile] = field(default_factory=list)
    errors: Dict[str, str] = field(default_factory=dict)
    warnings: Dict[str, str] = field(default_factory=dict)


class Collector(SparkTableAction, ABC):
    @abstractmethod
    def collect(self) -> FilesCollection:
        pass
