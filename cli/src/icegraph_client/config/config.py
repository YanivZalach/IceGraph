import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from icegraph_client.storage.storage import DEFAULT_DATA_DIR

CONFIG_FILENAME = "config.json"


class MissingServerUrlError(Exception):
    pass


@dataclass(frozen=True)
class CliConfig:
    server_url: str
    data_dir: Path

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "CliConfig":
        raw_data_dir = args.data_dir or os.environ.get("ICEGRAPH_DATA_DIR")
        data_dir = Path(raw_data_dir).expanduser() if raw_data_dir else DEFAULT_DATA_DIR

        server_url = args.server or os.environ.get("ICEGRAPH_SERVER_URL") or _load_saved_server_url(data_dir)
        if not server_url:
            server_url = _prompt_for_server_url()
            _save_server_url(data_dir, server_url)

        return cls(server_url=server_url, data_dir=data_dir)


def _config_path(data_dir: Path) -> Path:
    return data_dir / CONFIG_FILENAME


def _load_saved_server_url(data_dir: Path) -> Optional[str]:
    path = _config_path(data_dir)
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text()).get("server_url")
    except (json.JSONDecodeError, OSError):
        return None


def _save_server_url(data_dir: Path, server_url: str) -> None:
    path = _config_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"server_url": server_url}))


def _prompt_for_server_url() -> str:
    print(
        "No IceGraph server configured yet (pass --server or set ICEGRAPH_SERVER_URL to skip this prompt next time).",
        file=sys.stderr,
    )
    while True:
        try:
            value = input("IceGraph server URL: ").strip()
        except EOFError:
            raise MissingServerUrlError(
                "No IceGraph server configured and no terminal available to ask. "
                "Pass --server or set ICEGRAPH_SERVER_URL."
            )

        if value:
            return value

        print("A server URL is required.", file=sys.stderr)
