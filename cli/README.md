# icegraph-client

A Python client and CLI for [IceGraph](../README.md) — pull an Apache Iceberg table's metadata down from a running IceGraph server, inspect it from the terminal, and jump straight to the browser view.

It's a package first: `IcegraphClient` and the storage helpers have no CLI-framework or `print()` coupling, so anything (a script, a notebook, a future MCP server) can `import icegraph_client` and use them directly. The `icegraph` command is one interface built on top of that.

## Install

```bash
pip install icegraph-client
```

This installs the `icegraph` command and makes `icegraph_client` importable.

## Usage

```bash
icegraph tables
icegraph load default.logging
icegraph show default.logging
icegraph show default.logging --type snapshot --operation append
icegraph show default.logging --node <node id or a unique substring of it>
icegraph show default.logging --children <node id or a unique substring of it>
icegraph show default.logging --issues
icegraph show default.logging --json
icegraph open default.logging
```

`tables` and `load` print a status line to stderr while they wait on the server, so stdout stays clean for scripting.

`show` explores a table's cached graph:
- with no flags, lists every node as `type  id` (filterable with `--type`/`--operation`)
- `--node <id>` prints every field the backend returned for one node; matches an exact id or, if unambiguous, any substring of a node's id or label — handy since ids are full file paths
- `--children <id>` lists the nodes one node points to (its manifest's data files, a snapshot's manifests, etc.), matched the same way
- `--issues` prints errors/warnings instead of nodes
- add `--json` to any of the above for machine-readable output instead of formatted text — with no other mode/filter flags, `--json` dumps the entire cached result (`nodes`, `edges`, `metadata`, `errors`, `warnings`)

`open` opens the matching browser view in your default browser (rather than printing the URL); pass `--no-browser` to print the URL instead, and it also falls back to printing if no browser could be opened.

## Configuration

| Setting | Flag | Environment variable | Default |
|---|---|---|---|
| Server URL | `--server` | `ICEGRAPH_SERVER_URL` | Prompted for on first run (see below) |
| Local data directory | `--data-dir` | `ICEGRAPH_DATA_DIR` | `~/.icegraph` |

If no server URL is given via `--server` or `ICEGRAPH_SERVER_URL`, the CLI asks for one interactively the first time it runs, then saves it to `<data-dir>/config.json` so later runs don't ask again.

`load` persists a table's full graph-data response, gzip-compressed, to `<data-dir>/<table>/<start>-<end>.json.gz` (`None` in place of a bound that wasn't given) and records it as that table's latest load. `show`/`open` use the latest load automatically unless `--start`/`--end` are given explicitly.

## As a library

```python
from icegraph_client import IcegraphClient

client = IcegraphClient("http://your-icegraph-server:5000")
result = client.load_table("default.logging")
print(len(result["nodes"]), "nodes")
```

## Development

Working from source instead of the published package:

```bash
cd cli
uv sync                    # Install dependencies into cli/.venv
uv run icegraph --help      # Run the CLI without installing it
uv run pytest                # Run the test suite
```
