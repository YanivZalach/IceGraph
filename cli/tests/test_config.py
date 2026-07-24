import argparse
from pathlib import Path

from icegraph_client.client import DEFAULT_SERVER_URL
from icegraph_client.config import CliConfig
from icegraph_client.storage import DEFAULT_DATA_DIR


def _args(server=None, data_dir=None):
    return argparse.Namespace(server=server, data_dir=data_dir)


def test_server_url_flag_wins_over_env(monkeypatch):
    monkeypatch.setenv("ICEGRAPH_SERVER_URL", "http://env:1")
    config = CliConfig.from_args(_args(server="http://flag:2"))
    assert config.server_url == "http://flag:2"


def test_server_url_env_wins_over_default(monkeypatch):
    monkeypatch.setenv("ICEGRAPH_SERVER_URL", "http://env:1")
    config = CliConfig.from_args(_args())
    assert config.server_url == "http://env:1"


def test_server_url_default(monkeypatch):
    monkeypatch.delenv("ICEGRAPH_SERVER_URL", raising=False)
    config = CliConfig.from_args(_args())
    assert config.server_url == DEFAULT_SERVER_URL


def test_data_dir_default(monkeypatch):
    monkeypatch.delenv("ICEGRAPH_DATA_DIR", raising=False)
    config = CliConfig.from_args(_args())
    assert config.data_dir == DEFAULT_DATA_DIR


def test_data_dir_flag_wins_over_env(monkeypatch):
    monkeypatch.setenv("ICEGRAPH_DATA_DIR", "/env/dir")
    config = CliConfig.from_args(_args(data_dir="/flag/dir"))
    assert config.data_dir == Path("/flag/dir")


def test_data_dir_env_wins_over_default(monkeypatch):
    monkeypatch.setenv("ICEGRAPH_DATA_DIR", "/env/dir")
    config = CliConfig.from_args(_args())
    assert config.data_dir == Path("/env/dir")
