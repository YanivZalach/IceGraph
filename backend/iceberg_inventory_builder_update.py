import arrow
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from typing import Optional, Dict, Any
from spark_connect import open_spark_connect_session
from icegraph_logger import logger
from constants import FileType, UI_NEWLINE, MAIN_BRANCH_ICEBERG_TABLE_NAME
from utils import (
    to_spark_timestamp,
    get_metadata_row_df_from_path,
    get_json_metadata_from_path,
    _update_col_metric,
    format_partition,
    format_schemas_to_full_dict,
)

# Make the new build return more ganular data, and the normilizer will fix it.
# See if you get the undeterministic graph in react, i think its the JS, because the timestamp was good.
# Verify the build an no errors.
# Next version, give the ui the logs on what you do.

class IcebergInventoryBuilder:
    def __init__(self, full_table_name: str, date_to_view: Optional[str] = None):
        self._spark = open_spark_connect_session()
        self._table_name = full_table_name
        self._date_to_view = to_spark_timestamp(date_to_view) if date_to_view else None

        self._cached_dfs = []
        self._errors: Dict[str, str] = {}

        self._timestamp_cutoff = None
        self._manifests_to_ignore_df = None

        self._metadata_files = None
        self._main_metadata_file = None
        self._snapshot_rows = None
        self._snapshots = None
        self._manifests = None
        self._data_files = None

    def collect(self) -> Dict[str, Any]:
        total_start = time.time()

        self._timed("find_search_cutoff", self._find_search_cutoff)

        # metadata files and snapshots are independent — run in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            meta_future = executor.submit(
                self._timed, "collect_metadata_files", self._collect_metadata_files
            )
            snap_future = executor.submit(
                self._timed, "collect_snapshots", self._collect_snapshots
            )
            meta_future.result()
            snap_future.result()

        self._timed("link_metadata_to_snapshots", self._link_metadata_to_snapshots)
        self._timed("collect_manifests", self._collect_manifests)
        self._clean_cache()

        logger.info(
            f"[{self._table_name}] total collect took {time.time() - total_start:.2f}s"
        )
        return self._build_result()

    def _timed(self, name: str, fn):
        start = time.time()
        result = fn()
        logger.info(f"[{self._table_name}] {name} took {time.time() - start:.2f}s")
        return result

    def _build_result(self) -> Dict[str, Any]:
        inventory = (
            (self._metadata_files or [])
            + (self._snapshots or [])
            + (self._manifests or [])
            + (self._data_files or [])
        )

        metadata_specs = {"table-name": self._table_name}
        if self._main_metadata_file:
            main_meta_path = self._main_metadata_file["file"]
            try:
                content = get_json_metadata_from_path(main_meta_path)
                content["schemas"] = format_schemas_to_full_dict(
                    content.get("schemas", [])
                )
                content["table-name"] = self._table_name
                metadata_specs = content
            except Exception as e:
                self._errors[main_meta_path] = f"Metadata specs error: {e}"

        return {
            "inventory": inventory,
            "errors": self._errors,
            "metadata_specs": metadata_specs,
        }

    def _cache_df(self, df):
        df.cache()
        self._cached_dfs.append(df)

    def _clean_cache(self):
        for df in self._cached_dfs:
            df.unpersist()
        self._cached_dfs = []

    def _find_search_cutoff(self):
        if not self._date_to_view:
            self._set_no_cutoff()
            return

        baseline_snap_row = self._spark.sql(
            f"SELECT snapshot_id, committed_at FROM {self._table_name}.snapshots"
            f" WHERE committed_at < '{self._date_to_view}' ORDER BY committed_at DESC LIMIT 1"
        ).collect()

        if not baseline_snap_row:
            self._set_no_cutoff()
            return
        baseline_snap_row = baseline_snap_row[0]

        base_id = baseline_snap_row.snapshot_id
        self._timestamp_cutoff = baseline_snap_row.committed_at
        self._manifests_to_ignore_df = self._spark.sql(
            f"SELECT path FROM {self._table_name}.manifests VERSION AS OF {base_id}"
        )
        self._cache_df(self._manifests_to_ignore_df)

    def _set_no_cutoff(self):
        self._timestamp_cutoff = arrow.Arrow.min
        self._manifests_to_ignore_df = self._spark.createDataFrame(
            [], StructType([StructField("path", StringType())])
        )

    def _collect_metadata_files(self):
        metadata_df = (
            self._spark.sql(f"SELECT * FROM {self._table_name}.metadata_log_entries")
            .withColumnRenamed("timestamp", "metadata_timestamp")
            .select("file", "metadata_timestamp")
        )
        if self._timestamp_cutoff:
            metadata_df = metadata_df.filter(
                F.col("metadata_timestamp") > F.lit(str(self._timestamp_cutoff))
            )
        self._cache_df(metadata_df)

        metadata_files = {
            row.file: row.metadata_timestamp for row in metadata_df.collect()
        }

        metadata_files_df = None
        for file, timestamp in metadata_files.items():
            df = (
                get_metadata_row_df_from_path(file)
                .withColumn("metadata_timestamp", F.lit(timestamp))
                .withColumn("file", F.lit(file))
            )
            metadata_files_df = (
                df
                if metadata_files_df is None
                else metadata_files_df.unionByName(df, allowMissingColumns=True)
            )

        if metadata_files_df is None:
            return

        rows = metadata_files_df.orderBy(F.desc("metadata_timestamp")).collect()
        n = len(rows)
        self._metadata_files = []
        for index, row in enumerate(rows):
            is_main_metadata_file = index == 0
            if is_main_metadata_file:
                self._main_metadata_file = row.asDict(recursive=True)

            self._metadata_files.append(
                {
                    "type": (
                        FileType.MAIN_METADATA.value
                        if is_main_metadata_file
                        else FileType.METADATA.value
                    ),
                    "file_path": row.file,
                    "timestamp": str(row.metadata_timestamp),
                    "table_format_version": row["format-version"],
                    "snapshot_id": row["current-snapshot-id"],
                    "partition_spec_id": row["default-spec-id"],
                    "current_schema_id": row["current-schema-id"],
                    "sort_order_id": row["default-sort-order-id"],
                    "refs": json.loads(row.refs) if row.refs else {},
                    "properties": json.loads(row.properties) if row.properties else {},
                    "hidden_metadata": {
                        "color_append": 1 - index / (1.5 * n),
                    },
                }
            )

    def _link_metadata_to_snapshots(self):
        snap_id_to_path = {
            s["snapshot_id"]: s["file_path"] for s in (self._snapshots or [])
        }

        for meta in self._metadata_files or []:
            refs = meta.get("refs", {})

            # Main connection: current snapshot
            current_snap_path = snap_id_to_path.get(meta["snapshot_id"])
            child_files = [current_snap_path] if current_snap_path else []

            # Branch connections: all refs of type "branch" except the main branch
            branches = {
                name: attrs["snapshot-id"]
                for name, attrs in refs.items()
                if attrs.get("type") == "branch"
                and name != MAIN_BRANCH_ICEBERG_TABLE_NAME
            }

            # Group branch names by snapshot_id (multiple branches can share one snapshot)
            snapshot_to_branches = defaultdict(list)
            for branch_name, snap_id in branches.items():
                snapshot_to_branches[snap_id].append(branch_name)

            branch_files = {}
            for snap_id, branch_names in snapshot_to_branches.items():
                snap_path = snap_id_to_path.get(snap_id)
                if snap_path and snap_path not in child_files:
                    child_files.append(snap_path)
                branch_files[snap_path] = ", ".join(branch_names)

            meta["child_files"] = child_files
            meta["hidden_metadata"]["branch_files"] = branch_files

    def _collect_snapshots(self):
        snapshots_df = self._spark.sql(
            f"SELECT * FROM {self._table_name}.snapshots ORDER BY committed_at DESC"
        )
        if self._timestamp_cutoff:
            snapshots_df = snapshots_df.filter(
                F.col("committed_at") > F.lit(str(self._timestamp_cutoff))
            )
        self._snapshot_rows = snapshots_df.collect()

        self._snapshots = []
        for snapshot in self._snapshot_rows:
            summary = snapshot.summary or {}
            summary_repr = UI_NEWLINE.join(
                (
                    f"{k}: {(int(v) / (1024**3)):.5f} GB"
                    if k.endswith("files-size")
                    else f"{k}: {v}"
                )
                for k, v in summary.items()
            )
            self._snapshots.append(
                {
                    "type": FileType.SNAPSHOT.value,
                    "file_path": snapshot.manifest_list,
                    "timestamp": str(snapshot.committed_at),
                    "snapshot_id": snapshot.snapshot_id,
                    "parent_id": snapshot.parent_id,
                    "operation": snapshot.operation,
                    "summary": summary_repr,
                    "child_files": [],  # filled in _collect_manifests
                }
            )

    def _collect_manifests(self):
        if not self._snapshot_rows:
            self._manifests = []
            self._data_files = []
            return

        all_manifests_df = self._union_snapshot_manifests_df()
        if all_manifests_df is None:
            self._manifests = []
            self._data_files = []
            return

        manifest_rows = self._timed(
            "collect_manifest_rows",
            lambda: all_manifests_df.join(
                self._manifests_to_ignore_df, on="path", how="left_anti"
            ).collect(),
        )
        if not manifest_rows:
            self._manifests = []
            self._data_files = []
            return

        self._fill_snapshot_child_files(manifest_rows)

        seen_paths = set()
        deduped_manifest_rows = []
        for m in manifest_rows:
            if m.path not in seen_paths:
                seen_paths.add(m.path)
                deduped_manifest_rows.append(m)

        avro_entries = self._timed(
            "collect_avro_entries",
            lambda: self._collect_avro_entries(deduped_manifest_rows),
        )
        self._process_avro_entries(avro_entries, deduped_manifest_rows)

    def _union_snapshot_manifests_df(self):
        result = None
        for snapshot in self._snapshot_rows:
            snap_id = snapshot.snapshot_id
            df = self._spark.sql(
                f"SELECT *, {snap_id} as _snap_id"
                f" FROM {self._table_name}.manifests VERSION AS OF {snap_id}"
            )
            result = (
                df
                if result is None
                else result.unionByName(df, allowMissingColumns=True)
            )
        return result

    def _fill_snapshot_child_files(self, manifest_rows):
        snap_id_to_snapshot = {s["snapshot_id"]: s for s in self._snapshots}
        seen_per_snap = defaultdict(set)
        for m in manifest_rows:
            snap = snap_id_to_snapshot.get(m._snap_id)
            if snap and m.path not in seen_per_snap[m._snap_id]:
                snap["child_files"].append(m.path)
                seen_per_snap[m._snap_id].add(m.path)

    def _collect_avro_entries(self, manifest_rows):
        avro_df = None
        for m_row in manifest_rows:
            df = (
                self._spark.read.format("avro")
                .load(m_row.path)
                .select("status", "data_file")
                .withColumn("_manifest_path", F.lit(m_row.path))
            )
            avro_df = (
                df
                if avro_df is None
                else avro_df.unionByName(df, allowMissingColumns=True)
            )
        return avro_df.collect()

    def _process_avro_entries(self, avro_entries, manifest_rows):
        entries_by_manifest = defaultdict(list)
        for entry in avro_entries:
            entries_by_manifest[entry._manifest_path].append(entry)

        manifest_info = {m.path: m for m in manifest_rows}
        self._manifests = []
        self._data_files = []
        processed_data_files = set()

        for m_path, entries in entries_by_manifest.items():
            self._process_manifest(
                m_path, entries, manifest_info[m_path], processed_data_files
            )

    def _process_manifest(self, m_path, entries, m_row, processed_data_files):
        child_data_paths_status = {"existing": [], "deleted": []}
        total_rows = 0
        all_partitions = set()

        for entry in entries:
            f = entry["data_file"]
            f_path = f["file_path"]

            if entry["status"] == 2:
                child_data_paths_status["deleted"].append(f_path)
            else:
                child_data_paths_status["existing"].append(f_path)

            total_rows += f["record_count"]
            all_partitions.add(format_partition(f.partition))

            if f_path not in processed_data_files:
                self._data_files.append(self._format_data_file(f))
                processed_data_files.add(f_path)

        self._manifests.append(
            {
                "type": FileType.MANIFEST.value,
                "file_path": m_path,
                "added_snapshot_id": m_row.added_snapshot_id,
                "partitions": UI_NEWLINE.join(all_partitions),
                "total_rows": total_rows,
                "existing_child_files": child_data_paths_status["existing"],
                "deleted_child_files": child_data_paths_status["deleted"],
                "child_files": child_data_paths_status["existing"]
                + child_data_paths_status["deleted"],
            }
        )

    def _format_data_file(self, f):
        if f.content == 0:
            f_type = FileType.DATA.value
        elif f.content == 1:
            f_type = FileType.POSITION_DELETE.value
        else:
            f_type = FileType.EQUALITY_DELETE.value

        column_metrics = {}
        _update_col_metric(f.lower_bounds, "lower_bound", column_metrics)
        _update_col_metric(f.upper_bounds, "upper_bound", column_metrics)
        _update_col_metric(f.column_sizes, "size_bytes", column_metrics)
        _update_col_metric(f.null_value_counts, "null_count", column_metrics)
        _update_col_metric(f.nan_value_counts, "not_a_number_count", column_metrics)
        _update_col_metric(f.value_counts, "total_values", column_metrics)

        return {
            "type": f_type,
            "file_path": f["file_path"],
            "format": f.file_format,
            "size_gb": f"{(f.file_size_in_bytes / 1024 ** 3):.10f}",
            "row_count": f.record_count,
            "partition": format_partition(f.partition),
            "sort_order_id": f.sort_order_id,
            "columns": column_metrics,
            "split_offsets": UI_NEWLINE.join(map(str, f.split_offsets or [])),
            "key_metadata": f.key_metadata,
            "equality_ids": f.equality_ids,
        }