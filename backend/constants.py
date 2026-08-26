import inspect
from enum import Enum

APPLICATION_PORT = 5_050

MAIN_BRANCH_ICEBERG_TABLE_NAME = "main"

JOB_TOKEN_FIELD = "X-IceGraph-Job-Token"

STANDART_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

REPLACE_OPERATION = "replace"


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

DATA_FILES_CUTOFF_MANIFEST_WARNING = inspect.cleandoc("""
The data files of the manifest were not loaded/attached because the limit of {max_data_files_to_collect} data files was reached.
""")

MANIFESTS_LIMIT_ERROR = inspect.cleandoc("""
The number of manifests exceeds the limit of {max_manifests_to_collect}!
The manifests and the data files were not collected, select a smaller snapshot range.
""")

METADATA_FILES_CUTOFF_WARNING = inspect.cleandoc("""
Showing partial metadata! the number of metadata files exceeds the limit of {max_metadata_files_to_collect}!

Older metadata files were not collected.
Only the oldest metadata files are cut off, so every metadata file you see is complete and accurate.
""")
