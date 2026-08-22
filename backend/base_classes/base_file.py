from typing import List, Optional

from pydantic import BaseModel

from constants import FileType


class HiddenFile(BaseModel):
    pass


class BaseFile(BaseModel):
    type: FileType
    file_path: str
    child_files: List[str]
    error: Optional[str] = None
    warning: Optional[str] = None

    def to_dict(self):
        result_dict = {name: getattr(self, name) for name in type(self).model_fields if not isinstance(getattr(self, name), HiddenFile)}
        result_dict["type"] = result_dict["type"].value

        child_files = result_dict.pop("child_files")
        result_dict["child_files"] = child_files

        return result_dict
