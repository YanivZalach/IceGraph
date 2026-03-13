import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Any, Set

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    MapType,
)

from constants import (
    FileType,
    PARALLEL_SPARK_SQL,
    MAIN_BRANCH_ICEBERG_TABLE_NAME,
    UI_NEWLINE,
)
from icegraph_logger import logger
from utils import (
    to_spark_timestamp,
    get_json_metadata_from_path,
    _update_col_metric,
    format_partition,
    format_schemas_to_full_dict,
)


class IcebergInventoryBuilder:
    def __init__(self, full_table_name: str, date_to_view: Optional[str] = None):
        self.spark = SparkSession.builder.getOrCreate()
        self.table_name = full_table_name
        self.date_to_view = date_to_view

        self.metadata_file_content = None
        self.inventory: List[Dict[str, Any]] = []
        self.processed_data_files: Set[str] = set()
        self.processed_manifests: Set[str] = set()
        self.manifests_to_ignore: Set[str] = set()
        self.snapshots_to_path: Dict[str, int] = {}

        self.timestamp_cutoff = (
            to_spark_timestamp(date_to_view) if date_to_view else None
        )
        self.lock = threading.Lock()
        self.errors = {}
        self.manifest_cache: Dict[int, List[Dict[str, Any]]] = {}

    def collect(self) -> Dict[str, Any]:
        logger.info(f"Analyzing Table {self.table_name}")

        try:
            self.manifests_to_ignore = self._discover_baseline_manifests()
            df = self._load_metadata_and_snapshots()

            if self.timestamp_cutoff:
                df = df.filter(
                    F.coalesce(F.col("snapshot_timestamp"), F.col("meta_log_timestamp"))
                    >= F.lit(str(self.timestamp_cutoff)).cast("timestamp")
                )

            rows = df.sort(F.desc("meta_log_timestamp")).collect()

            self._prefetch_all_manifests(rows)

            for index, row in enumerate(rows):
                is_main_metadata_file = index == 0
                previous_metadata_file = None
                if index + 1 < len(rows):
                    previous_metadata_file = rows[index + 1].asDict().get("file")

                self._process_row(
                    row, is_main_metadata_file, previous_metadata_file, index, len(rows)
                )

        except Exception as e:
            self.errors[self.table_name] = f"Critical Table Error: {str(e)}"

        for file, msg in self.errors.items():
            logger.error(f"Error when processing file {file} - {msg}")

        self._connect_metadata_branches()

        result = {
            "inventory": self.inventory,
            "errors": self.errors,
            "metadata_specs": {},
        }

        if self.metadata_file_content:
            result["metadata_specs"] = {
                "table-name": self.table_name,
                "current-schema-id": self.metadata_file_content.get(
                    "current-schema-id"
                ),
                "schemas": format_schemas_to_full_dict(
                    self.metadata_file_content.get("schemas")
                ),
                "default-spec-id": self.metadata_file_content.get("default-spec-id"),
                "partition-specs": self.metadata_file_content.get("partition-specs"),
                "default-sort-order-id": self.metadata_file_content.get(
                    "default-sort-order-id"
                ),
                "sort-orders": self.metadata_file_content.get("sort-orders"),
            }

        return result

    def _connect_metadata_branches(self):
        for item in self.inventory:
            if item["type"] not in [
                FileType.METADATA.value,
                FileType.MAIN_METADATA.value,
            ]:
                continue

            branches = item["hidden_metadata"].get("branches", {})
            snapshot_to_branches = {}
            for branch_name, snapshot_id in branches.items():
                snapshot_to_branches.setdefault(snapshot_id, []).append(branch_name)

            label_to_snapshot = {
                ", ".join(names): snapshot_id
                for snapshot_id, names in snapshot_to_branches.items()
            }

            item["hidden_metadata"]["branch_files"] = {}
            for branch_names, snapshot_id in label_to_snapshot.items():
                if snapshot_id in self.snapshots_to_path:
                    item["child_files"].append(self.snapshots_to_path[snapshot_id])
                    item["hidden_metadata"]["branch_files"][
                        self.snapshots_to_path[snapshot_id]
                    ] = branch_names

    def _get_schema_info(self, meta_file: str):
        try:
            metadata = get_json_metadata_from_path(meta_file)

            refs = metadata.get("refs", {})
            branches = {
                branch_name: attrs["snapshot-id"]
                for branch_name, attrs in refs.items()
                if branch_name != MAIN_BRANCH_ICEBERG_TABLE_NAME
                and attrs.get("type") == "branch"
            }
            refs_str = UI_NEWLINE.join(
                f"{key}: {' | '.join(f'{k}={v}' for k, v in attrs.items())}"
                for key, attrs in refs.items()
            )

            properties = metadata.get("properties", {})
            properties_str = UI_NEWLINE.join(
                f'"{k}": "{v}"' for k, v in properties.items()
            )

            return (
                meta_file,
                metadata.get("format-version"),
                metadata.get("default-sort-order-id"),
                metadata.get("current-schema-id"),
                refs_str,
                branches,
                properties_str,
            )

        except Exception as e:
            with self.lock:
                self.errors[meta_file] = f"Error when processing file {meta_file}: {e}"
            return (meta_file, None, None, None, None, {}, {})

    def _load_metadata_and_snapshots(self):
        metadata_df = (
            self.spark.sql(f"SELECT * FROM {self.table_name}.metadata_log_entries")
            .withColumnRenamed("latest_snapshot_id", "snapshot_id")
            .withColumnRenamed("timestamp", "meta_log_timestamp")
        )

        metadata_files = [
            row["file"] for row in metadata_df.select("file").distinct().collect()
        ]

        if metadata_files:
            with ThreadPoolExecutor(max_workers=PARALLEL_SPARK_SQL) as executor:
                schema_results = list(
                    executor.map(self._get_schema_info, metadata_files)
                )
        else:
            schema_results = []

        schema = StructType(
            [
                StructField("file", StringType(), True),
                StructField("format_version", IntegerType(), True),
                StructField("default_sort_order_id", IntegerType(), True),
                StructField("current_schema_id", IntegerType(), True),
                StructField("refs", StringType(), True),
                StructField("branches", MapType(StringType(), LongType()), True),
                StructField("properties", StringType(), True),
            ]
        )
        schema_df = self.spark.createDataFrame(schema_results, schema=schema)

        metadata_df = metadata_df.join(schema_df, on="file", how="left")

        snapshots_df = self.spark.sql(
            f"SELECT * FROM {self.table_name}.snapshots"
        ).withColumnRenamed("committed_at", "snapshot_timestamp")

        return metadata_df.join(snapshots_df, on="snapshot_id", how="full")

    def _discover_baseline_manifests(self) -> Set[str]:
        if not self.timestamp_cutoff:
            return set()

        baseline_snap_row = self.spark.sql(
            f"SELECT snapshot_id FROM {self.table_name}.snapshots WHERE committed_at < '{self.timestamp_cutoff}' ORDER BY committed_at DESC LIMIT 1"
        ).collect()

        if not baseline_snap_row:
            return set()

        base_id = baseline_snap_row[0]["snapshot_id"]
        old_manifests = self.spark.sql(
            f"SELECT path FROM {self.table_name}.manifests VERSION AS OF {base_id}"
        ).collect()
        return {m["path"] for m in old_manifests}

    def _prefetch_all_manifests(self, rows: List[Any]):
        snap_ids = {
            row.asDict()["snapshot_id"]
            for row in rows
            if row.asDict().get("snapshot_id")
        }

        with ThreadPoolExecutor(max_workers=PARALLEL_SPARK_SQL) as executor:
            results = executor.map(self._fetch_snapshot_manifests, snap_ids)
            for snap_id, manifests in results:
                self.manifest_cache[snap_id] = manifests

    def _fetch_snapshot_manifests(self, snap_id: int):
        try:
            manifests = self.spark.sql(
                f"SELECT * FROM {self.table_name}.manifests VERSION AS OF {snap_id}"
            ).collect()
            return snap_id, manifests
        except Exception as e:
            with self.lock:
                self.errors[f"snap_{snap_id}"] = f"Pre-fetch SQL Error: {str(e)}"
            return snap_id, []

    def _process_row(
        self,
        row,
        is_main_metadata_file,
        previous_metadata_file,
        index,
        number_of_metadata_files,
    ):
        row_dict = row.asDict()
        snap_id = row_dict.get("snapshot_id")
        meta_file = row_dict.get("file")

        if meta_file:
            try:
                if is_main_metadata_file:
                    self.metadata_file_content = get_json_metadata_from_path(meta_file)

                self.inventory.append(
                    {
                        "type": (
                            FileType.MAIN_METADATA.value
                            if is_main_metadata_file
                            else FileType.METADATA.value
                        ),
                        "file_path": meta_file,
                        "timestamp": str(row_dict.get("meta_log_timestamp")),
                        "table_format_version": row_dict.get("format_version"),
                        "snapshot_id": snap_id,
                        "previous_metadata_file": previous_metadata_file,
                        "current_schema_id": row_dict.get("current_schema_id"),
                        "latest_writen_schema_id": row_dict.get("latest_schema_id"),
                        "default_sort_order_id": row_dict.get("default_sort_order_id"),
                        "latest_sequence_number": row_dict.get(
                            "latest_sequence_number"
                        ),
                        "refs": row_dict.get("refs"),
                        "properties": row_dict.get("properties"),
                        "child_files": (
                            [row_dict.get("manifest_list")]
                            if row_dict.get("manifest_list")
                            else []
                        ),
                        "hidden_metadata": {
                            "color_append": 1
                            - index / (1.5 * number_of_metadata_files),
                            "branches": row_dict.get("branches"),
                        },
                    }
                )
            except Exception as e:
                self.errors[meta_file] = f"Metadata Read Error: {str(e)}"

        if snap_id and row_dict.get("manifest_list"):
            snapshot_path = row_dict["manifest_list"]
            summary = row_dict.get("summary", {})
            summary_repr = UI_NEWLINE.join(
                [
                    (
                        f"{k}: {(int(v) / (1024**3)):.5f} GB"
                        if k.endswith("files-size")
                        else f"{k}: {v}"
                    )
                    for k, v in summary.items()
                ]
            )

            try:
                manifests = self.manifest_cache.get(snap_id, [])
                self.snapshots_to_path[snap_id] = snapshot_path

                self.inventory.append(
                    {
                        "type": FileType.SNAPSHOT.value,
                        "file_path": snapshot_path,
                        "timestamp": str(row_dict.get("snapshot_timestamp")),
                        "snapshot_id": snap_id,
                        "parent_id": row_dict.get("parent_id"),
                        "operation": row_dict["operation"],
                        "summary": summary_repr,
                        "child_files": [m["path"] for m in manifests],
                    }
                )
                self._process_manifests(manifests)
            except Exception as e:
                self.errors[snapshot_path] = f"Snapshot SQL Error: {str(e)}"

    def _process_manifests(self, manifests):
        with ThreadPoolExecutor(max_workers=PARALLEL_SPARK_SQL) as executor:
            executor.map(self._process_single_manifest, manifests)

    def _process_single_manifest(self, m_row):
        m_path = m_row["path"]

        # Quick check without lock for speed; we'll re-check inside the lock
        if m_path in self.manifests_to_ignore or m_path in self.processed_manifests:
            return

        try:
            entries = (
                self.spark.read.format("avro")
                .load(m_path)
                .select("status", "data_file")
                .collect()
            )

            child_data_paths_status = {"existing": [], "deleted": []}
            total_rows = 0
            all_partitions = set()
            local_new_data_files = []

            for entry in entries:
                f = entry["data_file"]
                f_path = f["file_path"]
                f_status = entry["status"]

                if f_status == 2:
                    child_data_paths_status["deleted"].append(f_path)
                else:
                    child_data_paths_status["existing"].append(f_path)

                total_rows += f["record_count"]
                partition_repr = format_partition(f.partition)
                all_partitions.add(partition_repr)

                if f.content == 0:
                    f_type = FileType.DATA.value
                elif f.content == 1:
                    f_type = FileType.POSITION_DELETE.value
                else:
                    f_type = FileType.EQUALITY_DELETE.value

                if f_path not in self.processed_data_files:
                    column_metrics = {}
                    _update_col_metric(f.lower_bounds, "lower_bound", column_metrics)
                    _update_col_metric(f.upper_bounds, "upper_bound", column_metrics)
                    _update_col_metric(f.column_sizes, "size_bytes", column_metrics)
                    _update_col_metric(
                        f.null_value_counts, "null_count", column_metrics
                    )
                    _update_col_metric(
                        f.nan_value_counts, "not_a_number_count", column_metrics
                    )
                    _update_col_metric(f.value_counts, "total_values", column_metrics)

                    local_new_data_files.append(
                        {
                            "type": f_type,
                            "file_path": f_path,
                            "format": f.file_format,
                            "size_gb": f"{(f.file_size_in_bytes / 1024 ** 3):.10f}",
                            "row_count": f.record_count,
                            "partition": partition_repr,
                            "sort_order_id": f.sort_order_id,
                            "columns": column_metrics,
                            "split_offsets": UI_NEWLINE.join(
                                map(str, f.split_offsets or [])
                            ),
                            "key_metadata": f.key_metadata,
                            "equality_ids": f.equality_ids,
                        }
                    )

            with self.lock:
                if m_path in self.processed_manifests:
                    return

                for data_file in local_new_data_files:
                    if data_file["file_path"] not in self.processed_data_files:
                        self.inventory.append(data_file)
                        self.processed_data_files.add(data_file["file_path"])

                self.inventory.append(
                    {
                        "type": FileType.MANIFEST.value,
                        "file_path": m_path,
                        "added_snapshot_id": m_row["added_snapshot_id"],
                        "partitions": UI_NEWLINE.join(all_partitions),
                        "total_rows": total_rows,
                        "existing_child_files": child_data_paths_status["existing"],
                        "deleted_child_files": child_data_paths_status["deleted"],
                        "child_files": child_data_paths_status["existing"]
                        + child_data_paths_status["deleted"],
                    }
                )
                self.processed_manifests.add(m_path)

        except Exception as e:
            with self.lock:
                self.errors[m_path] = f"Manifest Processing Error: {str(e)}"
