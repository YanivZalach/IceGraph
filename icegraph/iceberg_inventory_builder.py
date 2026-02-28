from typing import Optional, List, Dict, Any, Set

from pyspark.sql import SparkSession, functions as F

from constants import FileType
from utils import to_utc_timestamp


class IcebergInventoryBuilder:
    def __init__(self, full_table_name: str, date_to_view: Optional[str] = None):
        self.spark = SparkSession.builder.getOrCreate()
        self.table_name = full_table_name
        self.date_to_view = date_to_view

        # Internal State
        self.inventory: List[Dict[str, Any]] = []
        self.processed_data_files: Set[str] = set()  # To avoid duplicates
        self.processed_manifests: Set[str] = set()
        self.manifests_to_ignore: Set[str] = set()

        # Convert date to UTC cutoff once
        self.utc_cutoff = to_utc_timestamp(date_to_view) if date_to_view else None

    def collect(self) -> List[Dict[str, Any]]:
        print(f"Analyzing {self.table_name}...")

        self.manifests_to_ignore = self._discover_baseline_manifests()
        df = self._load_metadata_and_snapshots()

        if self.utc_cutoff:
            df = df.filter(
                F.coalesce(F.col("snapshot_timestamp"), F.col("meta_log_timestamp"))
                >= F.lit(str(self.utc_cutoff)).cast("timestamp")
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
        if not self.utc_cutoff:
            return set()

        baseline_snap_row = self.spark.sql(
            f"SELECT snapshot_id FROM {self.table_name}.snapshots WHERE committed_at < '{self.utc_cutoff}' ORDER BY committed_at DESC LIMIT 1"
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
                    "snapshot_id": snap_id,
                    "operation": row_dict["operation"],
                    "child_files": [m["path"] for m in manifests],
                }
            )
            self._process_manifests(manifests)

    def _process_manifests(self, manifests):
        for m_row in manifests:
            m_path = m_row["path"]
            if m_path in self.manifests_to_ignore or m_path in self.processed_manifests:
                continue

            # Read manifest entries (fast Avro read)
            entries = (
                self.spark.read.format("avro")
                .load(m_path)
                .select("status", "data_file")
                .collect()
            )

            child_data_paths = []
            total_rows = 0
            all_partitions = set()

            for entry in entries:
                f = entry["data_file"]
                f_path = f["file_path"]
                f_partition = f.partition.asDict() if f.partition else "Root"
                child_data_paths.append(f_path)
                total_rows += f["record_count"]
                all_partitions.add(str(f_partition))

                # DATA FILE NODE (only add if we haven't seen it yet)
                if f_path not in self.processed_data_files:
                    f_type = (
                        FileType.DATA.value if f.content == 0 else FileType.DELETE.value
                    )

                    # Pivot logic: Create a master dictionary of column metrics
                    column_metrics = {}

                    # helper to populate the pivot table
                    def update_col_metric(source_list, metric_name):
                        if not source_list:
                            return
                        for row in source_list:
                            col_id = row.key
                            if col_id not in column_metrics:
                                column_metrics[col_id] = {}
                            column_metrics[col_id][metric_name] = row.value

                    # Fill the metrics
                    update_col_metric(f.column_sizes, "size_bytes")
                    update_col_metric(f.null_value_counts, "null_count")
                    update_col_metric(f.nan_value_counts, "nan_count")
                    update_col_metric(f.value_counts, "total_values")

                    self.inventory.append(
                        {
                            "type": f_type,
                            "file_path": f_path,
                            "format": f.file_format,
                            "size_gb": f"{(f.file_size_in_bytes / 1024 ** 3):.10f}",
                            "row_count": f.record_count,
                            "partition": f_partition,
                            "spec_id": f.sort_order_id,
                            "columns": "The columns are by there id",
                            **column_metrics,
                        }
                    )
                    self.processed_data_files.add(f_path)

            # MANIFEST NODE
            self.inventory.append(
                {
                    "type": FileType.MANIFEST.value,
                    "file_path": m_path,
                    "added_snapshot_id": m_row["added_snapshot_id"],
                    "partitions": list(all_partitions),
                    "total_rows": total_rows,
                    "child_files": child_data_paths,
                }
            )
            self.processed_manifests.add(m_path)
