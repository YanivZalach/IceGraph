import json

import pytest

from icegraph_client.storage.storage import LocalStorage


def test_save_writes_file_and_pointer(tmp_path):
    storage = LocalStorage(tmp_path)
    result = {"nodes": [{"id": "a"}], "edges": []}

    path = storage.save("default.logging", None, None, result)

    assert path.exists()
    assert json.loads(path.read_text()) == result

    pointer = tmp_path / "default.logging" / "_latest.json"
    assert pointer.exists()
    pointer_data = json.loads(pointer.read_text())
    assert pointer_data["file"] == path.name
    assert "loaded_at" in pointer_data


def test_save_names_file_by_range(tmp_path):
    storage = LocalStorage(tmp_path)
    path = storage.save("default.logging", 1, 2, {"nodes": []})
    assert path.name == "1-2.json"


def test_save_names_file_none_for_missing_range(tmp_path):
    storage = LocalStorage(tmp_path)
    path = storage.save("default.logging", None, None, {"nodes": []})
    assert path.name == "None-None.json"


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
