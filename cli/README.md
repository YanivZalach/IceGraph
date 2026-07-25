# icegraph-client

A Python client and CLI for [IceGraph](../README.md) — pull an Apache Iceberg table's metadata down from a running IceGraph server, inspect it from the terminal, and jump straight to the browser view.

It's a package first: `IcegraphClient` and the storage helpers have no CLI-framework or `print()` coupling, so anything (a script, a notebook, a future MCP server) can `import icegraph_client` and use them directly. The `icegraph` command is one interface built on top of that.

## Install

```bash
pip install icegraph-client
```

This installs the `icegraph` command and makes `icegraph_client` importable.

## Usage

The flow: list tables, pick one, check its snapshot history, `load` a range, then explore that loaded range with `show`/`metadata`/`open` — none of which take a date/range argument themselves, since they always operate on whatever range you last loaded (or switched to with `use`).

```bash
icegraph tables
icegraph snapshots default.logging
icegraph load default.logging --start 2026-01-01 --end 2026-02-01
icegraph show default.logging
icegraph show default.logging --type snapshot --operation append
icegraph show default.logging --node <node id or a unique substring of it>
icegraph show default.logging --children <node id or a unique substring of it>
icegraph show default.logging --issues
icegraph show default.logging --json   # or -j
icegraph metadata default.logging
icegraph open default.logging
icegraph use default.logging                              # list every range you've loaded for this table, numbered
icegraph use default.logging --index 0                    # switch to a previously loaded range by its number
```

`tables` and `load` print a status line to stderr while they wait on the server, so stdout stays clean for scripting.

`snapshots <table>` lists a table's snapshot history as `timestamp  snapshot_id  operation` (add `-j`/`--json` for the raw mapping) — use it to find a timestamp to pass to `load`. The text listing displays timestamps in your machine's local timezone (the backend reports them in UTC); `-j`/`--json` returns the raw UTC values unchanged.

`load` is the only command that takes `--start`/`--end`, and each accepts either a snapshot ID or a date/timestamp — a plain numeric ID is used as-is with no extra network call; anything else is resolved automatically against the table's snapshot history: `--end` picks the latest snapshot at or before the given time, `--start` picks the earliest snapshot at or after it. A bare date (`2026-01-01`, no time part) is treated as spanning the whole day, so `--end 2026-01-01` includes everything committed that day rather than stopping at midnight. There's no need to match an exact timestamp or disambiguate — the nearest snapshot is always picked automatically.

Omitting `--start`/`--end` is handled differently for each, since they mean different things: **omitting `--start`** stays unbounded (`None`) — "from the beginning of history" is a valid, stable request on its own, so nothing is resolved or looked up. **Omitting `--end`** always resolves to a concrete number — the table's actual latest snapshot **right now** — rather than an open-ended "whatever's latest" marker, so a loaded range stays a fixed, trustworthy fact even if the table gets new snapshots later; run `load` again to pick up new data. A freshly created table with only its initial metadata file and no snapshots yet is still loadable: `--start` stays `None` as usual, and `--end` is saved as `empty` (since there's no snapshot yet to resolve it to) — any later `show`/`metadata`/`open`/`use` against that range prints a note suggesting you `load` again once the table has data.

The backend reports snapshot timestamps in UTC. A date/timestamp you type with no explicit UTC offset (e.g. `2026-01-01` or `2026-01-01T20:00:00`) is interpreted in **your machine's local timezone** and converted to UTC before comparing — so `--end 2026-01-01` means midnight-to-midnight in your own timezone, not UTC. Include an explicit offset (e.g. `2026-01-01T00:00:00+00:00`) to bypass local-timezone interpretation entirely.

`use <table>` with no `--index` lists every range you've loaded for that table, numbered `[0]`, `[1]`, ... (marking the current one). Pass `--index`/`-i <N>` to switch to one by its number — no need to remember exact IDs/timestamps.

`show` explores the currently loaded range's cached graph — it takes no date arguments, only filters:
- with no flags, lists every node as `type  id` (filterable with `--type`/`--operation`; `--type metadata` also includes `main_metadata` nodes)
- `--node <id>` prints every field the backend returned for one node; matches an exact id or, if unambiguous, any substring of a node's id or label — handy since ids are full file paths
- `--children <id>` lists the nodes one node points to (its manifest's data files, a snapshot's manifests, etc.), matched the same way
- `--issues` prints errors/warnings instead of nodes
- add `--json`/`-j` to any of the above for machine-readable output instead of formatted text — with no other mode/filter flags, it dumps the cached result's `nodes`, `edges`, `errors`, and `warnings`. It never includes the table's root `metadata` (schema/partition-spec/sort-order) — that's exclusive to the `metadata` command.

`metadata <table>` prints the table's schema, partition spec, and sort order as of the current loaded range — the `metadata` key at the root of what the server returns, not to be confused with `metadata`-type nodes in `show` — as pretty-printed JSON (add `-j`/`--json` for a compact single line). Like `show`/`open`, it reads from the current range and takes no date arguments and makes no server request; switch ranges with `use` first if you want a different one.

`open` opens the currently loaded range's browser view in your default browser (rather than printing the URL); pass `--no-browser` to print the URL instead, and it also falls back to printing if no browser could be opened. If nothing has been loaded yet, it opens the table with no range filter.

## Configuration

| Setting | Flag | Environment variable | Default |
|---|---|---|---|
| Server URL | `--server` | `ICEGRAPH_SERVER_URL` | Prompted for on first run (see below) |
| Local data directory | `--data-dir` | `ICEGRAPH_DATA_DIR` | `~/.icegraph` |
| Non-interactive mode | `--non-interactive` | `ICEGRAPH_NON_INTERACTIVE` | Off (auto-enabled whenever stdin isn't a terminal) |

If no server URL is given via `--server` or `ICEGRAPH_SERVER_URL`, the CLI asks for one interactively the first time it runs, then saves it to `<data-dir>/config.json` so later runs don't ask again.

**For scripts and AI/agent use:** always pass `--server`/`ICEGRAPH_SERVER_URL` explicitly rather than relying on the prompt. The CLI never blocks on `input()` when stdin isn't a real terminal — it fails immediately with a clear error instead — but pass `--non-interactive` (or set `ICEGRAPH_NON_INTERACTIVE=true`) for a hard guarantee it will never prompt, even if stdin happens to be attached to a real terminal.

`load` resolves `--start`/`--end` per the rules above (start may stay `None`; end always resolves to a number, or `empty` for a snapshot-less table) and persists a table's full graph-data response, gzip-compressed, to `<data-dir>/<table>/<start-label>-<end-label>.json.gz`, recording it as that table's current range. `show`/`metadata`/`open` always operate on the current range; use `use` to switch it to any other range you've already loaded.

`load` also takes `-j`/`--json`, for a one-shot load-and-explore: it prints the loaded result as compact JSON on stdout (same shape as `show --json` with no filters — everything except the table's root `metadata`) while the normal "Loaded N nodes" status line moves to stderr, so stdout is pure JSON. It still saves to disk as usual, so a later `show`/`use` sees the same range. Useful for scripts/agents that want the data in a single call instead of `load` then `show --json`.

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
