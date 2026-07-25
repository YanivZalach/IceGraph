import gzip
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import arrow

DEFAULT_DATA_DIR = Path.home() / ".icegraph"

# Marker stored in place of an end snapshot id when a table had no snapshots at all
# (only its initial metadata file) at load time -- distinct from `None`, which means
# "no upper bound was ever requested" (see CommandRunner._resolve_snapshot_ref).
EMPTY_TABLE_END = "empty"

Bound = Union[int, str, None]


class LocalStorage:
    def __init__(self, data_dir: Path = DEFAULT_DATA_DIR):
        self._data_dir = data_dir

    def save(
        self,
        table_name: str,
        start_snapshot_id: Optional[int],
        end_snapshot_id: Bound,
        result: Dict,
    ) -> Path:
        path = self._result_path(table_name, start_snapshot_id, end_snapshot_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(result, f)

        self._write_latest_pointer(table_name, path.name)
        return path

    def set_latest(self, table_name: str, start_snapshot_id: Optional[int], end_snapshot_id: Bound) -> Path:
        path = self._result_path(table_name, start_snapshot_id, end_snapshot_id)
        if not path.exists():
            raise FileNotFoundError(
                f"No loaded data for {table_name} with start={start_snapshot_id}, end={end_snapshot_id}. "
                f"Run `icegraph load {table_name} --start ... --end ...` first."
            )

        self._write_latest_pointer(table_name, path.name)
        return path

    def list_ranges(self, table_name: str) -> List[Tuple[Optional[int], Bound]]:
        table_dir = self._table_dir(table_name)
        if not table_dir.exists():
            return []

        ranges = (self._parse_range_filename(path.name) for path in table_dir.glob("*.json.gz"))
        return sorted(ranges, key=self._range_sort_key)

    @classmethod
    def _range_sort_key(cls, range_pair: Tuple[Optional[int], Bound]) -> Tuple[Tuple[int, int], Tuple[int, int]]:
        start, end = range_pair
        return (cls._bound_sort_rank(start), cls._bound_sort_rank(end))

    @staticmethod
    def _bound_sort_rank(value: Bound) -> Tuple[int, int]:
        # Real ids sort by value first; "empty" and None (neither comparable with an
        # int or each other) sort after, in a fixed order, so mixed lists never raise.
        if isinstance(value, int):
            return (0, value)
        if value == EMPTY_TABLE_END:
            return (1, 0)
        return (2, 0)

    def current_range(self, table_name: str) -> Optional[Tuple[Optional[int], Bound]]:
        filename = self._read_latest_filename(table_name)
        return self._parse_range_filename(filename) if filename is not None else None

    def resolve_path(
        self,
        table_name: str,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Bound = None,
    ) -> Path:
        if start_snapshot_id is not None or end_snapshot_id is not None:
            path = self._result_path(table_name, start_snapshot_id, end_snapshot_id)
            if not path.exists():
                raise FileNotFoundError(f"No loaded data for {table_name} in that range. Run `icegraph load {table_name}` first.")
            return path

        filename = self._read_latest_filename(table_name)
        if filename is None:
            raise FileNotFoundError(f"No loaded data for {table_name} yet. Run `icegraph load {table_name}` first.")

        return self._table_dir(table_name) / filename

    def _read_latest_filename(self, table_name: str) -> Optional[str]:
        pointer = self._latest_pointer_path(table_name)
        if not pointer.exists():
            return None

        return json.loads(pointer.read_text())["file"]

    def load(
        self,
        table_name: str,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Bound = None,
    ) -> Dict:
        path = self.resolve_path(table_name, start_snapshot_id, end_snapshot_id)
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)

    def _write_latest_pointer(self, table_name: str, filename: str) -> None:
        pointer = self._latest_pointer_path(table_name)
        pointer.write_text(
            json.dumps(
                {
                    "file": filename,
                    "loaded_at": arrow.utcnow().isoformat(),
                }
            )
        )

    def _table_dir(self, table_name: str) -> Path:
        return self._data_dir / table_name

    def _result_path(self, table_name: str, start_snapshot_id: Optional[int], end_snapshot_id: Bound) -> Path:
        filename = f"{self._range_label(start_snapshot_id)}-{self._range_label(end_snapshot_id)}.json.gz"
        return self._table_dir(table_name) / filename

    def _latest_pointer_path(self, table_name: str) -> Path:
        return self._table_dir(table_name) / "_latest.json"

    @staticmethod
    def _range_label(value: Bound) -> str:
        return str(value) if value is not None else "None"

    @staticmethod
    def _parse_bound_label(label: str) -> Bound:
        if label == "None":
            return None
        if label == EMPTY_TABLE_END:
            return EMPTY_TABLE_END
        return int(label)

    @classmethod
    def _parse_range_filename(cls, filename: str) -> Tuple[Optional[int], Bound]:
        stem = filename[: -len(".json.gz")]
        start_label, _, end_label = stem.partition("-")
        start = cls._parse_bound_label(start_label)
        end = cls._parse_bound_label(end_label)
        return start, end
