import arrow
import json
import threading
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
    to_arrow_tz,
    get_metadata_row_slim_df_from_path,
    get_json_metadata_from_path,
    update_col_metric,
    format_partition,
    format_schemas_to_full_dict,
)


class IcebergInventoryBuilder:
    def __init__(
        self,
        full_table_name: str,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
    ):
        self._spark = open_spark_connect_session()
        self._table_name = full_table_name
        self._start_snapshot_id = start_snapshot_id
        self._end_snapshot_id = end_snapshot_id

        self._spark_tz = self._spark.conf.get("spark.sql.session.timeZone")
        self._errors: Dict[str, str] = {}
        self._snapshots_lock = threading.Lock()

        self._start_snapshot_cutoff = None
        self._end_snapshot_cutoff = None
        self._start_metadata_cutoff = None
        self._end_metadata_cutoff = None
        self._manifests_to_ignore_df = None

        self._metadata_files = None
        self._main_metadata_file = None
        self._snapshot_rows = None
        self._snapshots = None
        self._manifests = None
        self._data_files = None

    def collect(self) -> Dict[str, Any]:
        total_start = time.time()

        try:
            self._timed("find_search_cutoff", self._find_search_cutoff)
        except Exception as e:
            logger.error(f"[{self._table_name}] find_search_cutoff failed: {e}")
            self._errors["find_search_cutoff"] = str(e)
            self._set_full_history_cutoff()

        try:
            self._timed("collect_snapshots", self._collect_snapshots)
        except Exception as e:
            logger.error(f"[{self._table_name}] collect_snapshots failed: {e}")
            self._errors["collect_snapshots"] = str(e)

        # metadata files and manifests are independent once snapshots are ready — run in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            meta_future = executor.submit(
                self._timed, "collect_metadata_files", self._collect_metadata_files
            )
            manifests_future = executor.submit(
                self._timed, "collect_manifests", self._collect_manifests
            )
            for name, future in [
                ("collect_metadata_files", meta_future),
                ("collect_manifests", manifests_future),
            ]:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"[{self._table_name}] {name} failed: {e}")
                    self._errors[name] = str(e)
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
                self._main_metadata_file["schemas"] = format_schemas_to_full_dict(
                    self._main_metadata_file.get("schemas", [])
                )
                self._main_metadata_file["table-name"] = self._table_name
                metadata_specs = self._main_metadata_file
            except Exception as e:
                self._errors[main_meta_path] = f"Metadata specs error: {e}"

        return {
            "inventory": inventory,
            "errors": self._errors,
            "metadata_specs": metadata_specs,
        }

    def _clean_cache(self):
        if self._manifests_to_ignore_df is not None:
            self._manifests_to_ignore_df.unpersist()

    def _find_search_cutoff(self):
        if not self._start_snapshot_id and not self._end_snapshot_id:
            return self._set_full_history_cutoff()

        if self._start_snapshot_id:
            self._set_start_cutoffs()
        else:
            self._start_snapshot_cutoff = arrow.Arrow.min
            self._start_metadata_cutoff = arrow.Arrow.min
            self._manifests_to_ignore_df = self._create_empty_manifests_to_ignore_df()

        if self._end_snapshot_id:
            self._set_end_cutoffs()
        else:
            self._end_snapshot_cutoff = arrow.Arrow.max
            self._end_metadata_cutoff = arrow.Arrow.max

        if self._start_snapshot_cutoff > self._end_snapshot_cutoff:
            raise ValueError("Start snapshot is after end snapshot")

    def _set_start_cutoffs(self):
        row = self._spark.sql(
            f"""
            SELECT date_format(committed_at, "yyyy-MM-dd'T'HH:mm:ss.SSS") AS committed_at, parent_id
            FROM {self._table_name}.snapshots
            WHERE snapshot_id = {self._start_snapshot_id}
        """
        ).first()

        if not row:
            self._start_snapshot_cutoff = arrow.Arrow.min
            self._start_metadata_cutoff = arrow.Arrow.min
            self._manifests_to_ignore_df = self._create_empty_manifests_to_ignore_df()
            return

        self._start_snapshot_cutoff = to_arrow_tz(row.committed_at, self._spark_tz)

        meta_row = self._spark.sql(
            f"""
            SELECT date_format(MIN(timestamp), "yyyy-MM-dd'T'HH:mm:ss.SSS") AS ts
            FROM {self._table_name}.metadata_log_entries
            WHERE latest_snapshot_id = {self._start_snapshot_id}
        """
        ).first()

        self._start_metadata_cutoff = (
            to_arrow_tz(meta_row.ts, self._spark_tz)
            if meta_row and meta_row.ts
            else self._start_snapshot_cutoff
        )

        parent_id = row.parent_id

        if parent_id is None:
            self._manifests_to_ignore_df = self._create_empty_manifests_to_ignore_df()
        else:
            try:
                self._manifests_to_ignore_df = self._spark.sql(
                    f"""
                    SELECT path
                    FROM {self._table_name}.manifests
                    VERSION AS OF {parent_id}
                """
                )
            except Exception:
                self._manifests_to_ignore_df = (
                    self._create_empty_manifests_to_ignore_df()
                )

    def _set_end_cutoffs(self):
        row = self._spark.sql(
            f"""
            SELECT date_format(committed_at, "yyyy-MM-dd'T'HH:mm:ss.SSS") AS committed_at
            FROM {self._table_name}.snapshots
            WHERE snapshot_id = {self._end_snapshot_id}
        """
        ).first()

        if not row:
            self._end_snapshot_cutoff = arrow.Arrow.max
            self._end_metadata_cutoff = arrow.Arrow.max
            return

        self._end_snapshot_cutoff = to_arrow_tz(row.committed_at, self._spark_tz)

        meta_row = self._spark.sql(
            f"""
            SELECT date_format(MAX(timestamp), "yyyy-MM-dd'T'HH:mm:ss.SSS") AS ts
            FROM {self._table_name}.metadata_log_entries
            WHERE latest_snapshot_id = {self._end_snapshot_id}
        """
        ).first()

        self._end_metadata_cutoff = (
            to_arrow_tz(meta_row.ts, self._spark_tz)
            if meta_row and meta_row.ts
            else self._end_snapshot_cutoff
        )

    def _create_empty_manifests_to_ignore_df(self):
        return self._spark.createDataFrame(
            [], StructType([StructField("path", StringType())])
        )

    def _set_full_history_cutoff(self):
        self._start_snapshot_cutoff = arrow.Arrow.min
        self._end_snapshot_cutoff = arrow.Arrow.max

        self._start_metadata_cutoff = arrow.Arrow.min
        self._end_metadata_cutoff = arrow.Arrow.max

        self._manifests_to_ignore_df = self._spark.createDataFrame(
            [], StructType([StructField("path", StringType())])
        )

    def _collect_metadata_files(self):
        metadata_df = (
            self._spark.sql(f"SELECT * FROM {self._table_name}.metadata_log_entries")
            .withColumnRenamed("timestamp", "metadata_timestamp")
            .select("file", "metadata_timestamp")
        )
        if self._start_metadata_cutoff:
            metadata_df = metadata_df.filter(
                F.col("metadata_timestamp") >= F.lit(str(self._start_metadata_cutoff))
            )
        if self._end_metadata_cutoff:
            metadata_df = metadata_df.filter(
                F.col("metadata_timestamp") <= F.lit(str(self._end_metadata_cutoff))
            )

        metadata_files = {
            row.file: row.metadata_timestamp for row in metadata_df.collect()
        }

        metadata_files_df = None
        for file, timestamp in metadata_files.items():
            try:
                df = (
                    get_metadata_row_slim_df_from_path(file)
                    .withColumn("metadata_timestamp", F.lit(timestamp))
                    .withColumn("file", F.lit(file))
                )
                metadata_files_df = (
                    df
                    if metadata_files_df is None
                    else metadata_files_df.unionByName(df, allowMissingColumns=True)
                )
            except Exception as e:
                self._errors[file] = f"Metadata file read error: {e}"

        if metadata_files_df is None:
            return

        rows = metadata_files_df.orderBy(F.desc("metadata_timestamp")).collect()
        n = len(rows)
        with self._snapshots_lock:
            snap_id_to_path = {
                s["snapshot_id"]: s["file_path"] for s in (self._snapshots or [])
            }
        self._metadata_files = []
        for index, row in enumerate(rows):
            is_main_metadata_file = index == 0
            if is_main_metadata_file:
                try:
                    self._main_metadata_file = get_json_metadata_from_path(row.file)
                    self._main_metadata_file["file"] = row.file
                except Exception as e:
                    self._errors[row.file] = f"Main metadata file read error: {e}"

            refs = json.loads(row.refs) if getattr(row, "refs", None) else {}

            current_snap_path = snap_id_to_path.get(row["current-snapshot-id"])
            child_files = [current_snap_path] if current_snap_path else []

            branches = {
                name: attrs["snapshot-id"]
                for name, attrs in refs.items()
                if attrs.get("type") == "branch"
                and name != MAIN_BRANCH_ICEBERG_TABLE_NAME
            }
            snapshot_to_branches = defaultdict(list)
            for branch_name, snap_id in branches.items():
                snapshot_to_branches[snap_id].append(branch_name)

            branch_files = {}
            for snap_id, branch_names in snapshot_to_branches.items():
                snap_path = snap_id_to_path.get(snap_id)
                if snap_path:
                    child_files.append(snap_path)
                    branch_files[snap_path] = ", ".join(branch_names)

            previous_file = rows[index + 1].file if index + 1 < n else None

            self._metadata_files.append(
                {
                    "type": (
                        FileType.MAIN_METADATA.value
                        if is_main_metadata_file
                        else FileType.METADATA.value
                    ),
                    "file_path": row.file,
                    "timestamp": str(row.metadata_timestamp),
                    "snapshot_id": row["current-snapshot-id"],
                    "previous_file": previous_file,
                    "last_sequence_number": (
                        row["last-sequence-number"]
                        if "last-sequence-number" in row
                        else None
                    ),
                    "partition_spec_id": row["default-spec-id"],
                    "current_schema_id": row["current-schema-id"],
                    "sort_order_id": row["default-sort-order-id"],
                    "refs": refs,
                    "properties": json.loads(row.properties) if row.properties else {},
                    "child_files": child_files,
                    "hidden_metadata": {
                        "color_append": 1 - index / (1.5 * n),
                        "branch_files": branch_files,
                    },
                }
            )

    def _collect_snapshots(self):
        snapshots_df = self._spark.sql(
            f"SELECT * FROM {self._table_name}.snapshots ORDER BY committed_at DESC"
        )
        if self._start_snapshot_cutoff:
            snapshots_df = snapshots_df.filter(
                F.col("committed_at") >= F.lit(str(self._start_snapshot_cutoff))
            )
        if self._end_snapshot_cutoff:
            snapshots_df = snapshots_df.filter(
                F.col("committed_at") <= F.lit(str(self._end_snapshot_cutoff))
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
        snap_id_to_paths = defaultdict(list)
        seen_per_snap = defaultdict(set)
        for m in manifest_rows:
            if m.path not in seen_per_snap[m._snap_id]:
                snap_id_to_paths[m._snap_id].append(m.path)
                seen_per_snap[m._snap_id].add(m.path)

        with self._snapshots_lock:
            snap_id_to_snapshot = {s["snapshot_id"]: s for s in self._snapshots}
            for snap_id, paths in snap_id_to_paths.items():
                snap = snap_id_to_snapshot.get(snap_id)
                if snap:
                    snap["child_files"].extend(paths)

    def _collect_avro_entries(self, manifest_rows):
        avro_df = None
        for m_row in manifest_rows:
            try:
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
            except Exception as e:
                self._errors[m_row.path] = f"Avro read error: {e}"
        return avro_df.collect() if avro_df is not None else []

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
        update_col_metric(f.lower_bounds, "lower_bound", column_metrics)
        update_col_metric(f.upper_bounds, "upper_bound", column_metrics)
        update_col_metric(f.column_sizes, "size_bytes", column_metrics)
        update_col_metric(f.null_value_counts, "null_count", column_metrics)
        update_col_metric(f.nan_value_counts, "not_a_number_count", column_metrics)
        update_col_metric(f.value_counts, "total_values", column_metrics)

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
