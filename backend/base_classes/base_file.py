from dataclasses import dataclass, field, fields
from typing import List, Optional

from constants import FileType


@dataclass
class HiddenFile:
    pass


@dataclass
class BaseFile:
    type: FileType
    file_path: str
    child_files: List[str]
    error: Optional[str] = field(default=None, kw_only=True)

    def to_dict(self):
        result_dict = {field.name: getattr(self, field.name) for field in fields(self) if not isinstance(getattr(self, field.name), HiddenFile)}
        result_dict["type"] = result_dict["type"].value

        child_files = result_dict.pop("child_files")
        result_dict["child_files"] = child_files

        return result_dict
