import gzip
import json

import pytest

from icegraph_client.storage.storage import EMPTY_TABLE_END, LocalStorage

GZIP_MAGIC = b"\x1f\x8b"


def test_save_writes_gzip_compressed_file_and_pointer(tmp_path):
    storage = LocalStorage(tmp_path)
    result = {"nodes": [{"id": "a"}], "edges": []}

    path = storage.save("default.logging", None, None, result)

    assert path.exists()
    assert path.read_bytes()[:2] == GZIP_MAGIC
    with gzip.open(path, "rt", encoding="utf-8") as f:
        assert json.load(f) == result

    pointer = tmp_path / "default.logging" / "_latest.json"
    assert pointer.exists()
    pointer_data = json.loads(pointer.read_text())
    assert pointer_data["file"] == path.name
    assert "loaded_at" in pointer_data


def test_save_names_file_by_range(tmp_path):
    storage = LocalStorage(tmp_path)
    path = storage.save("default.logging", 1, 2, {"nodes": []})
    assert path.name == "1-2.json.gz"


def test_save_names_file_none_for_missing_range(tmp_path):
    storage = LocalStorage(tmp_path)
    path = storage.save("default.logging", None, None, {"nodes": []})
    assert path.name == "None-None.json.gz"


def test_load_round_trips(tmp_path):
    storage = LocalStorage(tmp_path)
    result = {"nodes": [{"id": "a"}]}
    storage.save("default.logging", 1, 2, result)

    loaded = storage.load("default.logging", 1, 2)

    assert loaded == result


def test_resolve_path_uses_latest_pointer_when_no_range_given(tmp_path):
    storage = LocalStorage(tmp_path)
    expected_path = storage.save("default.logging", None, None, {"nodes": []})

    resolved = storage.resolve_path("default.logging", None, None)

    assert resolved == expected_path


def test_resolve_path_uses_latest_pointer_after_second_load(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.save("default.logging", 1, 2, {"nodes": []})
    second_path = storage.save("default.logging", 3, 4, {"nodes": []})

    resolved = storage.resolve_path("default.logging", None, None)

    assert resolved == second_path


def test_resolve_path_raises_when_nothing_loaded(tmp_path):
    storage = LocalStorage(tmp_path)

    with pytest.raises(FileNotFoundError):
        storage.resolve_path("default.logging", None, None)


def test_resolve_path_raises_for_missing_explicit_range(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.save("default.logging", None, None, {"nodes": []})

    with pytest.raises(FileNotFoundError):
        storage.resolve_path("default.logging", 1, 2)


def test_list_ranges_returns_all_loaded_ranges_sorted(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.save("default.logging", 300, 400, {"nodes": []})
    storage.save("default.logging", 100, 200, {"nodes": []})

    assert storage.list_ranges("default.logging") == [(100, 200), (300, 400)]


def test_list_ranges_handles_mix_of_bounded_and_unbounded_ranges(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.save("default.events", None, None, {"nodes": []})
    storage.save("default.events", 2170216877480741855, None, {"nodes": []})
    storage.save("default.events", 100, 200, {"nodes": []})

    # must not raise TypeError comparing None to int, and bounded ranges sort before unbounded ones
    assert storage.list_ranges("default.events") == [
        (100, 200),
        (2170216877480741855, None),
        (None, None),
    ]


def test_save_and_load_round_trip_empty_table_marker(tmp_path):
    storage = LocalStorage(tmp_path)

    path = storage.save("default.fresh", None, EMPTY_TABLE_END, {"nodes": []})

    assert path.name == f"None-{EMPTY_TABLE_END}.json.gz"
    assert storage.current_range("default.fresh") == (None, EMPTY_TABLE_END)
    assert storage.load("default.fresh") == {"nodes": []}


def test_list_ranges_handles_mix_of_int_none_and_empty_marker(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.save("default.events", None, None, {"nodes": []})
    storage.save("default.events", None, EMPTY_TABLE_END, {"nodes": []})
    storage.save("default.events", 100, 200, {"nodes": []})

    # must not raise comparing "empty"/None/int against each other
    assert storage.list_ranges("default.events") == [
        (100, 200),
        (None, EMPTY_TABLE_END),
        (None, None),
    ]


def test_list_ranges_empty_when_nothing_loaded(tmp_path):
    storage = LocalStorage(tmp_path)
    assert storage.list_ranges("default.logging") == []


def test_current_range_reflects_latest_pointer(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.save("default.logging", 100, 200, {"nodes": []})
    storage.save("default.logging", 300, 400, {"nodes": []})

    assert storage.current_range("default.logging") == (300, 400)


def test_current_range_none_when_nothing_loaded(tmp_path):
    storage = LocalStorage(tmp_path)
    assert storage.current_range("default.logging") is None


def test_set_latest_switches_pointer_to_existing_range(tmp_path):
    storage = LocalStorage(tmp_path)
    storage.save("default.logging", 100, 200, {"nodes": ["a"]})
    storage.save("default.logging", 300, 400, {"nodes": ["b"]})

    path = storage.set_latest("default.logging", 100, 200)

    assert storage.current_range("default.logging") == (100, 200)
    assert storage.load("default.logging") == {"nodes": ["a"]}
    assert path.name == "100-200.json.gz"


def test_set_latest_raises_when_range_not_loaded(tmp_path):
    storage = LocalStorage(tmp_path)

    with pytest.raises(FileNotFoundError):
        storage.set_latest("default.logging", 1, 2)
