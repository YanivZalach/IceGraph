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
icegraph --base-url http://<icegraph-server-host> snapshots <database.table> [--before-snapshot-id ID]
icegraph --base-url http://<icegraph-server-host> metadata <database.table>
icegraph --base-url http://<icegraph-server-host> sparkdesc <database.table>
icegraph --base-url http://<icegraph-server-host> graph <database.table> [--start-snapshot-id ID] [--end-snapshot-id ID]
```

`--base-url` also falls back to the `ICEGRAPH_BASE_URL` environment variable. If your server sits behind a proxy that requires auth, pass `--token`/`--cookie` (or `ICEGRAPH_TOKEN`/`ICEGRAPH_COOKIE`).

Each command prints its result as JSON on stdout; status messages go to stderr, so output pipes cleanly.

`snapshots` returns one page, starting from the table's newest snapshots, as `{"snapshots": [...], "next_before_snapshot_id": ...}`. Pass `next_before_snapshot_id` as `--before-snapshot-id` to get the next older page; it is `null` when no older snapshots remain.

`metadata` returns the table's latest metadata, plus `warnings`, which maps a source to a list of messages and is empty when there are none. For example, it warns when the table uses an Iceberg format version other than 2, which IceGraph doesn't fully support yet.

`sparkdesc` returns Spark's description of any table Spark can resolve, Iceberg or not, as `{"spark_schema": {...}, "spark_partitions": [...], "sections": [...], "properties": {...}, "warnings": {...}}`. Use it when another command reports that the table is not an Iceberg table.

## Python

```python
from icegraph_client import IceGraphClient

client = IceGraphClient("http://<icegraph-server-host>")
client.list_tables()
page = client.get_snapshot_map("database.table")
if page.next_before_snapshot_id is not None:  # None when no older snapshots remain
    client.get_snapshot_map("database.table", before_snapshot_id=page.next_before_snapshot_id)
client.get_table_metadata("database.table")
client.get_table_description("database.table")
client.get_graph("database.table", start_snapshot_id, end_snapshot_id)
```

## Docs

Product page: [https://yanivzalach.github.io/IceGraph-Site/](https://yanivzalach.github.io/IceGraph-Site/)

Full documentation, the IceGraph application, and the source code: [github.com/YanivZalach/IceGraph](https://github.com/YanivZalach/IceGraph)

## License

`icegraph-client` is licensed under the
[GNU Affero General Public License version 3 only](https://github.com/YanivZalach/IceGraph/blob/master/LICENSE).

Copyright (c) 2026 Yaniv Zalach and the IceGraph contributors.
