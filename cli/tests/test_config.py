import argparse
from pathlib import Path

import pytest

from icegraph_client.config.config import CliConfig, MissingServerUrlError
from icegraph_client.storage.storage import DEFAULT_DATA_DIR


def _args(server=None, data_dir=None):
    return argparse.Namespace(server=server, data_dir=data_dir)


def test_server_url_flag_wins_over_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ICEGRAPH_SERVER_URL", "http://env:1")
    config = CliConfig.from_args(_args(server="http://flag:2", data_dir=str(tmp_path)))
    assert config.server_url == "http://flag:2"


def test_server_url_env_wins_over_saved_config(monkeypatch, tmp_path):
    monkeypatch.setenv("ICEGRAPH_SERVER_URL", "http://env:1")
    config = CliConfig.from_args(_args(data_dir=str(tmp_path)))
    assert config.server_url == "http://env:1"


def test_server_url_prompts_when_unset(monkeypatch, tmp_path):
    monkeypatch.delenv("ICEGRAPH_SERVER_URL", raising=False)
    monkeypatch.setattr("builtins.input", lambda prompt="": "http://typed:3")

    config = CliConfig.from_args(_args(data_dir=str(tmp_path)))

    assert config.server_url == "http://typed:3"


def test_server_url_prompt_rejects_blank_and_retries(monkeypatch, tmp_path):
    monkeypatch.delenv("ICEGRAPH_SERVER_URL", raising=False)
    responses = iter(["", "   ", "http://typed:3"])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(responses))

    config = CliConfig.from_args(_args(data_dir=str(tmp_path)))

    assert config.server_url == "http://typed:3"


def test_server_url_saved_after_first_prompt_and_not_asked_again(monkeypatch, tmp_path):
    monkeypatch.delenv("ICEGRAPH_SERVER_URL", raising=False)
    monkeypatch.setattr("builtins.input", lambda prompt="": "http://typed:3")

    CliConfig.from_args(_args(data_dir=str(tmp_path)))

    def _fail_if_called(prompt=""):
        raise AssertionError("should not prompt again once a server URL is saved")

    monkeypatch.setattr("builtins.input", _fail_if_called)
    config = CliConfig.from_args(_args(data_dir=str(tmp_path)))

    assert config.server_url == "http://typed:3"


def test_server_url_raises_when_no_terminal_available(monkeypatch, tmp_path):
    monkeypatch.delenv("ICEGRAPH_SERVER_URL", raising=False)

    def _raise_eof(prompt=""):
        raise EOFError

    monkeypatch.setattr("builtins.input", _raise_eof)

    with pytest.raises(MissingServerUrlError):
        CliConfig.from_args(_args(data_dir=str(tmp_path)))


def test_data_dir_default(monkeypatch):
    monkeypatch.delenv("ICEGRAPH_DATA_DIR", raising=False)
    config = CliConfig.from_args(_args(server="http://x:1"))
    assert config.data_dir == DEFAULT_DATA_DIR


def test_data_dir_flag_wins_over_env(monkeypatch):
    monkeypatch.setenv("ICEGRAPH_DATA_DIR", "/env/dir")
    config = CliConfig.from_args(_args(server="http://x:1", data_dir="/flag/dir"))
    assert config.data_dir == Path("/flag/dir")


def test_data_dir_env_wins_over_default(monkeypatch):
    monkeypatch.setenv("ICEGRAPH_DATA_DIR", "/env/dir")
    config = CliConfig.from_args(_args(server="http://x:1"))
    assert config.data_dir == Path("/env/dir")
