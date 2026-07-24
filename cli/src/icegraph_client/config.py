from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path

from .client import DEFAULT_SERVER_URL
from .storage import DEFAULT_DATA_DIR


@dataclass(frozen=True)
class CliConfig:
    server_url: str
    data_dir: Path

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "CliConfig":
        server_url = args.server or os.environ.get("ICEGRAPH_SERVER_URL", DEFAULT_SERVER_URL)

        raw_data_dir = args.data_dir or os.environ.get("ICEGRAPH_DATA_DIR")
        data_dir = Path(raw_data_dir).expanduser() if raw_data_dir else DEFAULT_DATA_DIR

        return cls(server_url=server_url, data_dir=data_dir)
