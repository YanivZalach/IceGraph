from enum import Enum

MAX_SNAPSHOTS_TO_SHOW = 10000

APPLICATION_PORT = 5000

PARALLEL_SPARK_SQL = 50

MAIN_BRANCH_ICEBERG_TABLE_NAME = "main"
UI_SECTION_NEWLINE = "\x00"
UI_NEWLINE = "\n"


class FileType(Enum):
    MAIN_METADATA = "main_metadata"
    METADATA = "metadata"
    SNAPSHOT = "snapshot"
    MANIFEST = "manifest"
    DATA = "data"
    POSITION_DELETE = "position_delete"
    EQUALITY_DELETE = "equality_delete"
