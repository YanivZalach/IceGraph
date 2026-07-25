import gzip
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

DEFAULT_DATA_DIR = Path.home() / ".icegraph"


class LocalStorage:
    def __init__(self, data_dir: Path = DEFAULT_DATA_DIR):
        self._data_dir = data_dir

    def save(
        self,
        table_name: str,
        start_snapshot_id: Optional[int],
        end_snapshot_id: Optional[int],
        result: Dict,
    ) -> Path:
        path = self._result_path(table_name, start_snapshot_id, end_snapshot_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(result, f)

        self._write_latest_pointer(table_name, path.name)
        return path

    def set_latest(self, table_name: str, start_snapshot_id: Optional[int], end_snapshot_id: Optional[int]) -> Path:
        path = self._result_path(table_name, start_snapshot_id, end_snapshot_id)
        if not path.exists():
            raise FileNotFoundError(
                f"No loaded data for {table_name} with start={start_snapshot_id}, end={end_snapshot_id}. "
                f"Run `icegraph load {table_name} --start ... --end ...` first."
            )

        self._write_latest_pointer(table_name, path.name)
        return path

    def list_ranges(self, table_name: str) -> List[Tuple[Optional[int], Optional[int]]]:
        table_dir = self._table_dir(table_name)
        if not table_dir.exists():
            return []

        return sorted(
            self._parse_range_filename(path.name)
            for path in table_dir.glob("*.json.gz")
        )

    def current_range(self, table_name: str) -> Optional[Tuple[Optional[int], Optional[int]]]:
        pointer = self._latest_pointer_path(table_name)
        if not pointer.exists():
            return None

        pointer_data = json.loads(pointer.read_text())
        return self._parse_range_filename(pointer_data["file"])

    def resolve_path(
        self,
        table_name: str,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
    ) -> Path:
        if start_snapshot_id is not None or end_snapshot_id is not None:
            path = self._result_path(table_name, start_snapshot_id, end_snapshot_id)
            if not path.exists():
                raise FileNotFoundError(f"No loaded data for {table_name} in that range. Run `icegraph load {table_name}` first.")
            return path

        pointer = self._latest_pointer_path(table_name)
        if not pointer.exists():
            raise FileNotFoundError(f"No loaded data for {table_name} yet. Run `icegraph load {table_name}` first.")

        pointer_data = json.loads(pointer.read_text())
        return self._table_dir(table_name) / pointer_data["file"]

    def load(
        self,
        table_name: str,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
    ) -> Dict:
        path = self.resolve_path(table_name, start_snapshot_id, end_snapshot_id)
        with gzip.open(path, "rt", encoding="utf-8") as f:
            return json.load(f)

    def _write_latest_pointer(self, table_name: str, filename: str) -> None:
        pointer = self._latest_pointer_path(table_name)
        pointer.write_text(json.dumps({
            "file": filename,
            "loaded_at": datetime.now(timezone.utc).isoformat(),
        }))

    def _table_dir(self, table_name: str) -> Path:
        return self._data_dir / table_name

    def _result_path(self, table_name: str, start_snapshot_id: Optional[int], end_snapshot_id: Optional[int]) -> Path:
        filename = f"{self._range_label(start_snapshot_id)}-{self._range_label(end_snapshot_id)}.json.gz"
        return self._table_dir(table_name) / filename

    def _latest_pointer_path(self, table_name: str) -> Path:
        return self._table_dir(table_name) / "_latest.json"

    @staticmethod
    def _range_label(value: Optional[int]) -> str:
        return str(value) if value is not None else "None"

    @staticmethod
    def _parse_range_filename(filename: str) -> Tuple[Optional[int], Optional[int]]:
        stem = filename[: -len(".json.gz")]
        start_label, _, end_label = stem.partition("-")
        start = None if start_label == "None" else int(start_label)
        end = None if end_label == "None" else int(end_label)
        return start, end
