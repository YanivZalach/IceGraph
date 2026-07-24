# icegraph-client

A Python client and CLI for [IceGraph](../README.md) — pull an Apache Iceberg table's metadata down from a running IceGraph server, inspect it from the terminal, and jump straight to the browser view.

It's a package first: `IcegraphClient` and the storage helpers have no CLI-framework or `print()` coupling, so anything (a script, a notebook, a future MCP server) can `import icegraph_client` and use them directly. The `icegraph` command is one interface built on top of that.

## Install

```bash
cd cli
pip install -e .
```

This installs the `icegraph` command and makes `icegraph_client` importable.

## Usage

```bash
icegraph tables
icegraph load default.logging
icegraph show default.logging
icegraph show default.logging --type snapshot --operation append
icegraph open default.logging
```

## Configuration

| Setting | Flag | Environment variable | Default |
|---|---|---|---|
| Server URL | `--server` | `ICEGRAPH_SERVER_URL` | `http://localhost:5000` |
| Local data directory | `--data-dir` | `ICEGRAPH_DATA_DIR` | `~/.icegraph` |

`load` persists a table's full graph-data response to `<data-dir>/<table>/<start>-<end>.json` (`None` in place of a bound that wasn't given) and records it as that table's latest load. `show`/`open` use the latest load automatically unless `--start`/`--end` are given explicitly.

## As a library

```python
from icegraph_client import IcegraphClient

client = IcegraphClient("http://localhost:5000")
result = client.load_table("default.logging")
print(len(result["nodes"]), "nodes")
```

## Development

```bash
cd cli
uv sync
uv run pytest
```
