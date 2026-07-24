import inspect
from enum import Enum

MAX_NUMBER_OF_GRAPHS_TO_COMPUTE = 15

MAX_SNAPSHOTS_TO_SHOW = 20

MAX_SNAPSHOTS_TO_COMPUTE = 50

COMPUTE_CLEANUP_TIME_SECONDS = 12

MAX_DATA_FILES_TO_COLLECT = 5_000

TABLE_LIST_CACHE_TTL_SECONDS = 60

INCLUDE_NONE_ICEBERG_CATALOGS = "true"

MAX_GRACEFUL_SHUTDOWN_TIME_SECONDS = 10

APPLICATION_PORT = 5_000

PRODUCTION_MODE = "false"

WSGI_THREADS = 20

MAIN_BRANCH_ICEBERG_TABLE_NAME = "main"
UI_NEWLINE = "\n"

STANDART_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"


class FileType(Enum):
    MAIN_METADATA = "main_metadata"
    METADATA = "metadata"
    SNAPSHOT = "snapshot"
    MANIFEST = "manifest"
    DATA = "data"
    POSITION_DELETE = "position_delete"
    EQUALITY_DELETE = "equality_delete"


DATA_FILES_CUTOFF_WARNING = inspect.cleandoc("""
Showing partial data! the number of data files exceeds the limit of {max_data_files_to_collect}!

Latest snapshot that got cut off (Meaning snapshots above it are included):
ID: {added_snapshot_id}
Timestamp: {added_snapshot_timestamp} UTC

The cutoff is applied at the snapshot boundary — all data files belonging to cut-off snapshots are excluded,
unless a newer visible snapshot also references them, in which case they are included.
Every data file you see is referenced by at least one snapshot that is newer than the cut-off snapshot.
""")
