import inspect
from enum import Enum

MAIN_BRANCH_ICEBERG_TABLE_NAME = "main"

JOB_TOKEN_FIELD = "X-IceGraph-Job-Token"

STANDART_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSSSS"

REPLACE_OPERATION = "replace"

STAGE_COLLECT_SNAPSHOTS = "Collecting snapshots"
STAGE_COLLECT_METADATA_FILES = "Collecting metadata files"
STAGE_COLLECT_TABLE_STATISTICS = "Collecting table statistics files"
STAGE_COLLECT_PARTITION_STATISTICS = "Collecting partition statistics files"
STAGE_COLLECT_MANIFESTS = "Collecting manifests"
STAGE_COLLECT_DATA_FILES = "Collecting data files"
STAGE_BUILD_GRAPH = "Building graph"

COLLECTION_STAGES = [
    STAGE_COLLECT_SNAPSHOTS,
    STAGE_COLLECT_METADATA_FILES,
    STAGE_COLLECT_TABLE_STATISTICS,
    STAGE_COLLECT_PARTITION_STATISTICS,
    STAGE_COLLECT_MANIFESTS,
    STAGE_COLLECT_DATA_FILES,
    STAGE_BUILD_GRAPH,
]


class FileType(Enum):
    MAIN_METADATA = "main_metadata"
    METADATA = "metadata"
    SNAPSHOT = "snapshot"
    MANIFEST = "manifest"
    DATA = "data"
    POSITION_DELETE = "position_delete"
    EQUALITY_DELETE = "equality_delete"
    TABLE_STATISTICS = "table_statistics"
    PARTITION_STATISTICS = "partition_statistics"


DATA_FILES_CUTOFF_WARNING = inspect.cleandoc("""
Showing partial data! the number of data files exceeds the limit of {max_data_files_to_collect}!

Cut-off point (this snapshot and all snapshots committed at the same time or earlier are cut off):
ID: {added_snapshot_id}
Timestamp: {added_snapshot_timestamp} UTC

The cutoff is applied at the snapshot boundary — all data files belonging to cut-off snapshots are excluded,
unless a newer visible snapshot also references them, in which case they are included.
Every data file you see is referenced by at least one snapshot that is newer than the cut-off snapshot.
""")

DATA_FILES_CUTOFF_UNKNOWN_WARNING = inspect.cleandoc("""
Showing partial data! the number of data files exceeds the limit of {max_data_files_to_collect}!

The snapshots shown also carry data files inherited from older snapshots that were removed from the table history (expired).
Those inherited data files are hidden due to the limit, all data files added by the snapshots shown are included.
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

STATISTICS_BEFORE_RANGE_ERROR = inspect.cleandoc("""
Failed to read the metadata file before the selected range: {metadata_file}
{statistics_name} files that existed before the range may be shown as added by the oldest metadata file in view.
""")

TABLE_STATISTICS_COLLECTION_ERROR = "Failed to read the table statistics entries, so table statistics files are not shown: {error}"

STATISTICS_ATTRIBUTION_WARNING = inspect.cleandoc("""
Some metadata files could not be read. {statistics_name} files are still shown, but IceGraph may not reliably identify which metadata file first added them.
Statistics files first observed after an unreadable metadata file may be linked to a later metadata version.
""")
