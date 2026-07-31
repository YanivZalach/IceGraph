# icegraph-client

Python client and CLI for the [IceGraph](https://github.com/YanivZalach/IceGraph) server API - script access to tables, snapshot history, and the metadata graph.

## Install

`icegraph-client` is only guaranteed compatible with the exact version of the IceGraph server it talks to - pin to that version:

```bash
pip install icegraph-client==<version>
```

## CLI

```bash
icegraph --base-url http://<icegraph-server-host> tables
icegraph --base-url http://<icegraph-server-host> snapshots <database.table>
icegraph --base-url http://<icegraph-server-host> graph <database.table> [--start-snapshot-id ID] [--end-snapshot-id ID]
```

`--base-url` also falls back to the `ICEGRAPH_BASE_URL` environment variable. If your server sits behind a proxy that requires auth, pass `--token`/`--cookie` (or `ICEGRAPH_TOKEN`/`ICEGRAPH_COOKIE`).

Each command prints its result as JSON on stdout; status messages go to stderr, so output pipes cleanly.

## Python

```python
from icegraph_client import IceGraphClient

client = IceGraphClient("http://<icegraph-server-host>")
client.list_tables()
client.get_snapshot_map("database.table")
client.get_graph("database.table", start_snapshot_id, end_snapshot_id)
```

## Docs

Full documentation, the IceGraph application, and the source code: [github.com/YanivZalach/IceGraph](https://github.com/YanivZalach/IceGraph)
