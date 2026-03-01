import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Dict, Any, Set

from pyspark.sql import SparkSession, functions as F

from constants import FileType
from utils import (
    to_spark_timestamp,
    get_json_metadata_from_path,
    _update_col_metric,
)

MAX_WORKERS = 50


class IcebergInventoryBuilder:
    def __init__(self, full_table_name: str, date_to_view: Optional[str] = None):
        self.spark = SparkSession.builder.getOrCreate()
        self.table_name = full_table_name
        self.date_to_view = date_to_view

        # Internal State
        self.metadata_file_content = None
        self.inventory: List[Dict[str, Any]] = []
        self.processed_data_files: Set[str] = set()  # To avoid duplicates
        self.processed_manifests: Set[str] = set()
        self.manifests_to_ignore: Set[str] = set()

        self.timestamp_cutoff = (
            to_spark_timestamp(date_to_view) if date_to_view else None
        )
        self.lock = threading.Lock()

    def collect(self) -> List[Dict[str, Any]]:
        print(f"Analyzing {self.table_name}...")

        self.manifests_to_ignore = self._discover_baseline_manifests()
        df = self._load_metadata_and_snapshots()

        if self.timestamp_cutoff:
            df = df.filter(
                F.col("snapshot_timestamp")
                >= F.lit(str(self.timestamp_cutoff)).cast("timestamp")
            )

        rows = df.sort(F.desc("meta_log_timestamp")).collect()
        for index, row in enumerate(rows):
            is_main_metadata_file = index == 0
            previous_metadata_file = None
            if index + 1 < len(rows):
                previous_metadata_file = rows[index + 1].asDict().get("file")

            self._process_row(
                row, is_main_metadata_file, previous_metadata_file, index, len(rows)
            )

        return self.inventory

    def _load_metadata_and_snapshots(self):
        metadata_df = (
            self.spark.sql(f"SELECT * FROM {self.table_name}.metadata_log_entries")
            .withColumnRenamed("latest_snapshot_id", "snapshot_id")
            .withColumnRenamed("timestamp", "meta_log_timestamp")
        )
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

        # METADATA NODE
        if meta_file:
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
                    "snapshot_id": snap_id,
                    "previous_metadata_file": previous_metadata_file,
                    "child_files": (
                        [row_dict.get("manifest_list")]
                        if row_dict.get("manifest_list")
                        else []
                    ),
                    "hidden_metadata": {
                        "color_append": 1 - index / (1.5 * number_of_metadata_files)
                    },
                }
            )

        # SNAPSHOT NODE
        if snap_id and row_dict.get("manifest_list"):
            manifests = self.spark.sql(
                f"SELECT * FROM {self.table_name}.manifests VERSION AS OF {snap_id}"
            ).collect()

            self.inventory.append(
                {
                    "type": FileType.SNAPSHOT.value,
                    "file_path": row_dict["manifest_list"],
                    "timestamp": str(row_dict.get("snapshot_timestamp")),
                    "snapshot_id": snap_id,
                    "operation": row_dict["operation"],
                    "child_files": [m["path"] for m in manifests],
                }
            )
            self._process_manifests(manifests)

    def _process_manifests(self, manifests):
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            executor.map(self._process_single_manifest, manifests)

    def _process_single_manifest(self, m_row):
        m_path = m_row["path"]

        # Quick check without lock for speed; we'll re-check inside the lock
        if m_path in self.manifests_to_ignore or m_path in self.processed_manifests:
            return

        entries = (
            self.spark.read.format("avro")
            .load(m_path)
            .select("status", "data_file")
            .collect()
        )

        child_data_paths = []
        total_rows = 0
        all_partitions = set()
        local_new_data_files = []

        for entry in entries:
            f = entry["data_file"]
            f_path = f["file_path"]
            f_partition = f.partition.asDict() if f.partition else {"Root": "Root"}
            child_data_paths.append(f_path)
            total_rows += f["record_count"]
            partition_repr = "|".join(
                f"'{key}'='{value}'" for key, value in f_partition.items()
            )
            all_partitions.add(partition_repr)

            if f_path not in self.processed_data_files:
                f_type = (
                    FileType.DATA.value if f.content == 0 else FileType.DELETE.value
                )

                column_metrics = {}
                _update_col_metric(f.column_sizes, "size_bytes", column_metrics)
                _update_col_metric(f.lower_bounds, "lower_bound", column_metrics)
                _update_col_metric(f.upper_bounds, "upper_bound", column_metrics)
                _update_col_metric(f.column_sizes, "size_bytes", column_metrics)
                _update_col_metric(f.null_value_counts, "null_count", column_metrics)
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
                        "spec_id": f.sort_order_id,
                        "columns": column_metrics,
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
                    "partitions": ",".join(all_partitions),
                    "total_rows": total_rows,
                    "child_files": child_data_paths,
                }
            )
            self.processed_manifests.add(m_path)
