import inspect

STAGE_READ_TABLE_METADATA = "Reading table metadata"
STAGE_PRUNE_MANIFESTS = "Pruning manifests"
STAGE_READ_MANIFEST_ENTRIES = "Reading manifest entries"
STAGE_BUILD_FILE_NODES = "Building file nodes"

PARTITION_FILES_STAGES = [
    STAGE_READ_TABLE_METADATA,
    STAGE_PRUNE_MANIFESTS,
    STAGE_READ_MANIFEST_ENTRIES,
    STAGE_BUILD_FILE_NODES,
]

PARTITION_MATCH_MATCHED = "matched"
PARTITION_MATCH_UNDECIDED = "undecided"

MANIFEST_CONTENT_DATA = 0
MANIFEST_ENTRY_STATUS_ADDED = 1
MANIFEST_ENTRY_STATUS_DELETED = 2

FILE_CONTENT_DATA = 0

VOID_TRANSFORM = "void"
LEGACY_PARTITION_FIELD_ID_START = 1000

SPARK_SQL_FIELD = "spark_sql"

PARTITION_FILES_LIMIT_ERROR = inspect.cleandoc("""
The partition has more than {limit} {kind} at snapshot {snapshot_id}, so IceGraph did not load it.
Run the Spark SQL query below in your Spark session to list every file of the partition.
""")

UNKNOWN_PARTITION_FIELD_ERROR = "Partition field '{name}' is not part of any partition spec of the table."
UNSUPPORTED_PARTITION_TYPE_ERROR = "Filtering on partition field '{name}' is not supported: its partition values have type {field_type}."
INVALID_PARTITION_VALUE_ERROR = "The value '{value}' is not a valid {field_type} for partition field '{name}'."
MISSING_SNAPSHOT_ERROR = "Snapshot {snapshot_id} is not in the current metadata of table {table_name}."
MISSING_CURRENT_SNAPSHOT_ERROR = "Table {table_name} has no current snapshot, so it has no partition files."
INLINE_MANIFESTS_ERROR = "Snapshot {snapshot_id} has inline v1 manifests, which are not supported by this version of IceGraph."
UNKNOWN_PARTITION_SPEC_ERROR = "Manifest {manifest_path} uses partition spec {spec_id}, which is not in the table metadata."
EMPTY_PARTITION_FILTER_ERROR = "At least one partition field value is required."
