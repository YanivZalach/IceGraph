from typing import Any, Dict, List, Optional

import polars as pl

from base_classes.utils import epoch_millis_to_arrow_utc, timed, to_arrow_utc
from collectors.collect_snapshots import CollectSnapshots
from collectors.collector import FilesCollection
from constants import SNAPSHOT_METADATA_WINDOW_WARNING
from env import Env
from icegraph_logger import logger
from search_cutoff.find_search_cutoff import SearchCutoff
from trino_backend.metadata_log_entries import TrinoMetadataLogEntriesExtractor
from trino_backend.raw_files import RawFileTable, read_raw_files
from trino_backend.trino_client import Row, metadata_table, run_query, timestamp_bound, utc_text

MANIFEST_PATHS_FILES = RawFileTable("manifest_paths", "AVRO", "manifest_path varchar")


class TrinoCollectSnapshots(CollectSnapshots):
    @timed
    def collect(self) -> FilesCollection:
        warnings = {}
        try:
            rows = self._query_snapshot_rows()
            if len(rows) > Env.MAX_SNAPSHOTS_TO_COMPUTE:
                raise ValueError(f"Too many snapshots to compute. Maximum is {Env.MAX_SNAPSHOTS_TO_COMPUTE}.")

            self._snapshots = []
            inline_manifest_warnings = []
            for row in rows:
                if not row.manifest_list:
                    inline_manifest_warnings.append(
                        f"Snapshot {row.snapshot_id} has inline v1 manifests, which are not supported by this version of IceGraph."
                    )
                    continue

                self._snapshots.append(self._parse_snapshot_row(row))
            if inline_manifest_warnings:
                warnings["inline_manifests"] = inline_manifest_warnings

        except Exception as e:
            logger.error(f"[{self._table_name}] snapshots collection failed", exc_info=True)
            self._errors["snapshot_collection"] = [str(e)]

        return FilesCollection(files=self._snapshots, errors=self._errors, warnings=warnings)

    def _query_snapshot_rows(self):
        return run_query(f"""
            SELECT {utc_text("committed_at")} AS committed_at, snapshot_id, parent_id, operation, manifest_list, summary
            FROM {metadata_table(self._table_name, "snapshots")}
            WHERE {timestamp_bound("committed_at", ">=", self._start_snapshot_cutoff)}
                AND {timestamp_bound("committed_at", "<=", self._end_snapshot_cutoff)}
            ORDER BY committed_at DESC
            LIMIT {Env.MAX_SNAPSHOTS_TO_COMPUTE + 1}
        """)


@timed
def collect_snapshot_map(table_name: str, max_snapshots_to_show: int, before_snapshot_id: Optional[int] = None) -> Dict[str, Any]:
    snapshots_table = metadata_table(table_name, "snapshots")
    older_filter = "TRUE"
    if before_snapshot_id is not None:
        before_rows = run_query(f"SELECT committed_at FROM {snapshots_table} WHERE snapshot_id = {int(before_snapshot_id)}")
        if not before_rows:
            raise ValueError(f"Snapshot {before_snapshot_id} was not found in the table history. It may have expired.")

        before_committed_at = f"(SELECT committed_at FROM {snapshots_table} WHERE snapshot_id = {int(before_snapshot_id)})"
        older_filter = f"(committed_at < {before_committed_at} OR (committed_at = {before_committed_at} AND snapshot_id < {int(before_snapshot_id)}))"

    rows = run_query(f"""
        SELECT {utc_text("committed_at")} AS snapshot_timestamp, snapshot_id, operation
        FROM {snapshots_table}
        WHERE {older_filter}
        ORDER BY committed_at DESC, snapshot_id DESC
        LIMIT {max_snapshots_to_show + 1}
    """)
    page_rows = rows[:max_snapshots_to_show]

    next_before_snapshot_id = None
    if len(rows) > max_snapshots_to_show and page_rows:
        next_before_snapshot_id = str(page_rows[-1].snapshot_id)

    return {
        "snapshots": {
            to_arrow_utc(row.snapshot_timestamp).isoformat(): {"snapshot_id": str(row.snapshot_id), "operation": row.operation} for row in page_rows
        },
        "next_before_snapshot_id": next_before_snapshot_id,
    }


@timed
def find_search_cutoff(table_name: str, start_snapshot_id: Optional[int], end_snapshot_id: Optional[int]) -> SearchCutoff:
    cutoff = SearchCutoff(manifests_to_ignore_df=set())
    metadata_log_entries_extractor = TrinoMetadataLogEntriesExtractor(table_name)

    if start_snapshot_id:
        start_row = _query_snapshot_row(table_name, start_snapshot_id)
        if not start_row:
            raise ValueError(f"Start snapshot {start_snapshot_id} does not exist in table {table_name}")

        cutoff.start_snapshot_cutoff = to_arrow_utc(start_row.committed_at)
        metadata_entries = metadata_log_entries_extractor.extract_entries().filter(pl.col("snapshot_id") == start_snapshot_id)
        metadata_timestamp_ms = metadata_entries["timestamp_ms"].min()
        if metadata_timestamp_ms is not None:
            cutoff.start_metadata_cutoff = epoch_millis_to_arrow_utc(metadata_timestamp_ms)
        else:
            _warn_commit_time_boundary(cutoff.warnings, "start", start_snapshot_id, metadata_log_entries_extractor.missing_snapshot_reason)
            cutoff.start_metadata_cutoff = cutoff.start_snapshot_cutoff
        cutoff.manifests_to_ignore_df = _get_manifests_to_ignore(table_name, start_row)

    if end_snapshot_id:
        end_row = _query_snapshot_row(table_name, end_snapshot_id)
        if not end_row:
            raise ValueError(f"End snapshot {end_snapshot_id} does not exist in table {table_name}")

        cutoff.end_snapshot_cutoff = to_arrow_utc(end_row.committed_at)
        metadata_entries = metadata_log_entries_extractor.extract_entries().filter(pl.col("snapshot_id") == end_snapshot_id)
        metadata_timestamp_ms = metadata_entries["timestamp_ms"].max()
        if metadata_timestamp_ms is not None:
            cutoff.end_metadata_cutoff = epoch_millis_to_arrow_utc(metadata_timestamp_ms)
        else:
            _warn_commit_time_boundary(cutoff.warnings, "end", end_snapshot_id, metadata_log_entries_extractor.missing_snapshot_reason)
            cutoff.end_metadata_cutoff = cutoff.end_snapshot_cutoff

    if cutoff.start_snapshot_cutoff > cutoff.end_snapshot_cutoff:
        raise ValueError("Start snapshot is after end snapshot")

    return cutoff


def _warn_commit_time_boundary(warnings: Dict[str, List[str]], label: str, snapshot_id: int, reason: str) -> None:
    warnings[f"{label}_snapshot_metadata_window"] = [SNAPSHOT_METADATA_WINDOW_WARNING.format(label=label, snapshot_id=snapshot_id, reason=reason)]


def _query_snapshot_row(table_name: str, snapshot_id: int) -> Optional[Row]:
    snapshots_table = metadata_table(table_name, "snapshots")
    rows = run_query(f"""
        SELECT
            {utc_text("snapshot.committed_at")} AS committed_at,
            snapshot.parent_id,
            (SELECT manifest_list FROM {snapshots_table} WHERE snapshot_id = snapshot.parent_id) AS parent_manifest_list
        FROM {snapshots_table} AS snapshot
        WHERE snapshot.snapshot_id = {int(snapshot_id)}
    """)

    return rows[0] if rows else None


def _get_manifests_to_ignore(table_name: str, start_row: Row) -> set:
    if start_row.parent_id is None or not start_row.parent_manifest_list:
        return set()

    try:
        return {row.manifest_path for row in read_raw_files(MANIFEST_PATHS_FILES, [start_row.parent_manifest_list], "manifest_path")}

    except Exception:
        logger.warning(
            f"[{table_name}] Failed to load parent manifest list for snapshot {start_row.parent_id} . Likely already cleaned up, which is expected",
            exc_info=True,
        )

        return set()
