---
name: icegraph
description: Use the icegraph-client CLI to talk to a remote IceGraph server — list tables, fetch snapshot history, and build the metadata graph — and construct working deep-link URLs into the IceGraph web UI (e.g. pointing at a specific table, snapshot range, or graph node). Use when the user references an IceGraph server or table, asks to inspect/debug an Iceberg table via command line, or wants a link/URL that shows them a specific view or node instead of raw JSON.
user-invocable: true
---

# icegraph

Wraps the `icegraph` CLI (from the `icegraph-client` PyPI package) for scripted access to an
IceGraph server, and covers how to turn what the CLI returns into a working link into the actual
IceGraph web UI. The CLI and the UI talk to the exact same backend API — this skill treats them as
one workflow, not two separate tools.

While using this skill, act as a big-data expert in Iceberg table metadata, infrastructure, and
engineering — not just a relay for CLI output. Interpret schemas, partition specs, snapshot
lineage, and errors/warnings with real domain judgment (what a given partition transform implies
for query planning, what a sequence-number or orphaned-file issue suggests actually went wrong,
why a given schema evolution or table property is or isn't a good idea). Summarize and explain,
don't just paste JSON back at the user.

**Naming:** table names are `catalog → database → table`. The `table` argument the CLI/UI take is
the fully-qualified `database.table` (add the catalog prefix only when the user is on a non-default
catalog: `catalog.database.table`). Always call the middle part the **database** — never abbreviate
it to "db" or guess at a shortened form. Use the exact name the user gives you, or whatever a
`tables` call actually returned — don't assume a database is named "default" or anything else.

## 0. The server is remote — never assume otherwise

IceGraph servers almost never run on the same machine as this session. Unless the user has already
told you a `--base-url` / `$ICEGRAPH_BASE_URL` this conversation, **ask for it** — don't guess
`localhost` and don't try to auto-start anything. The only exception is when you're clearly doing
IceGraph *development* work inside this repo itself (e.g. `docker_demo` is running) — only then is
suggesting a local URL reasonable.

Once you have it, reuse it for the rest of the task instead of re-asking per command.

## 1. Prerequisites

- Confirm `icegraph` is on PATH with `icegraph --help` before relying on it.
- **If it's not installed, don't just suggest a bare `pip install icegraph-client`.** Check
  `<base_url>/docs` first — the Overview section shows a **Version** field — and suggest the
  version-pinned install command instead: `pip install icegraph-client==<version>`. The client is
  only guaranteed compatible with the matching server release, so pinning matters. Only fall back
  to suggesting an unpinned install (and mention that caveat) if the docs page isn't reachable
  either. Suggest the command for the user to run — don't install it yourself.

## 2. Commands

```
icegraph [--base-url URL] [--token TOKEN] [--cookie COOKIE] [--no-verify-ssl] tables
icegraph [...] snapshots <table>
icegraph [...] graph <table> [-s/--start-snapshot-id ID] [-e/--end-snapshot-id ID]
```

Every flag also has an env var fallback: `ICEGRAPH_BASE_URL`, `ICEGRAPH_TOKEN`, `ICEGRAPH_COOKIE`,
`ICEGRAPH_NO_VERIFY_SSL`.
Each command prints its result as **JSON on stdout only** — status messages ("Fetching tables...",
etc.) go to stderr, so stdout is always safe to parse directly.

**Output shapes:**

- `tables` → a JSON array of table name strings.
- `snapshots <table>` → a JSON array of `{timestamp, snapshot_id, operation}` objects (timestamp is
  ISO 8601, converted to local time).
- `graph <table>` → `{nodes: [...], metadata: {...}, issues: {errors: {...}, warnings: {...}}}`.
  Each entry in `nodes` is one file's fields as a flat dict, with no wrapper object around them, so
  a field is read directly as `node["summary"]`. Every node carries `file_path`, `type`, and
  `child_files`, plus fields specific to its type. A node's identity for
  linking purposes is its `file_path`, the underlying Iceberg file's storage path. **If you call
  `graph` with no start/end snapshot id, it auto-selects only the latest snapshot** (the CLI prints
  which one to stderr) — don't assume it covers full history unless you passed a range.
  When summarizing a `graph` result to the user, lead with `issues.errors`, then `issues.warnings`,
  before general structure/counts — errors are what the Issues panel in the UI leads with too.

**For "what's my table's schema/properties/partition spec" questions, use the top-level `metadata`
key, not a node's own fields.** `metadata` is close to a raw dump of the table's current
`metadata.json` (original Iceberg field names, hyphenated) — `schemas` (full column definitions),
`partition-specs`, `sort-orders`, `properties`, `refs`, `format-version`, `location`, `table-uuid`,
`current-schema-id`, `default-spec-id`, etc. A metadata-file *node*, by contrast,
only carries IDs (`current_schema_id`, `partition_spec_id`, `sort_order_id`) plus `properties`/
`refs` — it does **not** contain the actual schema or partition-spec definitions, so it's the wrong
place to look for those.

**Failure modes:**

- Exit code `2` with "No IceGraph server address was given" → the base URL wasn't resolved; go
  back to step 0.
- Exit code `1` with `Error: ...` on stderr → the request itself failed (network, auth rejection,
  bad table name, etc.). Don't retry blindly — surface the message and move to step 3 if it looks
  auth-related (connection refused, 401/403), or step 3a if it looks TLS-related (`SSLError`,
  `CERTIFICATE_VERIFY_FAILED`, "self-signed certificate", etc.).

## 3. If a command fails and it looks like an auth problem

**Try without a token/cookie first** — most servers don't need one. IceGraph itself has no login
system; a token or cookie is only needed when whoever runs the server has put it behind their own
outside proxy. If a request looks rejected for that reason:

- If the user can already reach the IceGraph UI in a browser, that proxy has already authenticated
  their session — the value is sitting in that browser. Tell them to open DevTools →
  Application/Storage → Cookies (or Network tab → any request → Request Headers) on the IceGraph
  page to find the cookie or `Authorization` value in use.
- Otherwise, tell them to check with whoever set up their IceGraph server — the proxy is theirs,
  IceGraph has no visibility into it.
- Once they give you a value, pass it via `--token`/`--cookie` or `$ICEGRAPH_TOKEN`/`$ICEGRAPH_COOKIE`.

Never guess or fabricate a token/cookie value.

## 3a. If a command fails and it looks like a TLS/SSL problem

An `SSLError`/`CERTIFICATE_VERIFY_FAILED`/"self-signed certificate" failure means the server's TLS
certificate isn't trusted by this machine, most often a self-signed or internal-CA cert on an
internal IceGraph server.

**Never add `--no-verify-ssl` on your own.** It disables certificate verification for the request,
which has real security implications (no protection against a MITM'd connection) — explain that
plainly and ask the user whether they want to proceed with it before you re-run the command.
Only add it after they say yes; if they decline, stop and suggest they fix the underlying cert
trust instead (import the internal CA, use the correct hostname, etc.) — that's their call, not
something to solve for them.

## 4. Building a link into the web UI

Don't wait to be asked — default to including one. Practically any answer about a specific table,
snapshot, node, error, or schema is more useful with a link the user can actually look at, so treat
including one as the norm and leaving it out as the exception, not the other way around. Treat
"show me"/"link me to" as confirmation you're on the right track, not as a precondition. The only
time to skip it is when there's genuinely nothing to point at — a bare `tables` listing with no
particular table in focus.

```
<base_url>/table/graph?table=<uri-encoded database.table>&start_snapshot_id=<id>&end_snapshot_id=<id>&select_node_id=<uri-encoded file_path>
```

- Reuse the exact same `base_url` you already resolved for the CLI. There is no extra path prefix
  to guess — the `/IceGraph` prefix only exists on the public GitHub Pages *demo* build (a static,
  mocked deployment); a real server has none.
- URI-encode `table` and `select_node_id`.
- `select_node_id` is a node's Iceberg file path — get it from a `graph` command's `nodes` output
  (or a filename the user already gave you). It stays valid as long as that file is still part of
  the selected snapshot range.

**Three hard rules:**

1. **Always set both `start_snapshot_id` and `end_snapshot_id` — never emit `end_snapshot_id`
   alone.** A link with no `start_snapshot_id` opens onto an unbounded range instead of the
   specific, particular view you're actually pointing at. Resolve a concrete start the same way
   you resolved the data you're describing: if the answer came from a ranged `snapshots`/`graph`
   call, reuse that same range; if it came from a rangeless `graph <table>` call (which auto-selects
   only the latest snapshot), set `start_snapshot_id` equal to that same latest snapshot id, so the
   link reproduces that exact single-snapshot view rather than defaulting to full history.
2. **`select_node_id` only works on `/table/graph`.** `MetadataPage`, `TimelinePage`, and
   `FileTreePage` don't read it — they ignore it silently. If the user's request maps more
   naturally to one of those views, link to that page (`/table/metadata`, `/table/timeline`,
   `/table/filetree`, same `table`/snapshot query params) *without* `select_node_id`, and say
   plainly that node-level selection is only available on the Graph view.
3. **Never add `dup` or `cache_id` to a URL you construct.** Those params belong to the app's own
   "Duplicate tab" / "View in graph" buttons, which cache the current in-memory data into that
   browser's IndexedDB for about two seconds before deleting it. A URL built with them from outside
   the running app will fail with "No cached data found" for anyone who opens it. Only ever emit
   the plain form above.

Don't invent table names, snapshot ids, or node ids — they must come from an actual CLI call or
from what the user told you directly.

**If you have browser automation available** (e.g. Playwright tools), don't just show the link —
open it too, so the user is actually looking at the view you're describing instead of having to
click it themselves. Still always include the link text in your reply as well, since opening a
browser tab isn't a substitute for giving the user something they can re-open, copy, or share.

## 5. Tracing a snapshot back to the Spark job that wrote it

A `graph` node with `type: "snapshot"` may have an `action_link` that points directly to the Spark
History Server application that wrote it. Check `action_link` first. If it is populated, give that
link to the user directly. Do not ask for the History Server base URL or an example application
URL when the data already provides `action_link`.

The node also has a `summary` dict, a raw, unfiltered passthrough of whatever Iceberg/Spark put in
that snapshot's summary map. When `action_link` is empty, resolve the Spark application ID in this
order:

1. Use `summary["app-id"]` when `summary["engine-name"]` is `"spark"`.
2. Otherwise, use `summary["spark.app.id"]` when present.

If either form provides an application ID, tell the user they can look up that application in
their **Spark History Server**. Ask the user for an example URL to any application's page in their
Spark History Server (they likely have one bookmarked, or can grab one from another job's logs).
Then build the equivalent URL for this application ID by substituting just the app-id portion of
that example URL, leaving the rest of its structure (host, port, path shape) untouched. Don't
assume a fixed Spark History Server URL scheme, since it varies by setup.

## 6. Cache a table's data to a tmp file for the rest of the session

The first time you fetch a table's data with `snapshots` or `graph` in a conversation, write the
raw JSON to a tmp file (session scratch/tmp directory if one is available, otherwise a plain
system tmp path — name it so it's identifiable, e.g. `icegraph_<database>_<table>.json`). For any
follow-up question about that same table and range, including schema, properties, partition spec,
a specific node's fields, errors/warnings, or tracing a Spark application ID, read back from that
file instead of re-running the CLI. This data doesn't change from one question to the next within a
session, so re-fetching it is wasted round-trips to a remote server.

**Exception: never answer from the cache when the user is specifically asking about the table's
most recent activity** — e.g. "what are the last few operations," "what's the latest snapshot,"
"has anything written to this table since X." Re-run `snapshots`/`graph` fresh every time for that
kind of question, since new commits could have landed on the real table since the file was cached,
and giving a stale answer there is actively misleading in a way a stale schema/properties answer
usually isn't.

## Example flow

1. "What tables are on my IceGraph server at `https://ice.example.com`?"
   → `icegraph --base-url https://ice.example.com tables`
2. "Show me the snapshot history for `sales.orders`."
   → `icegraph --base-url https://ice.example.com snapshots sales.orders`
3. "Why does `sales.orders` have errors?" (no link requested)
   → `icegraph --base-url https://ice.example.com graph sales.orders` — no range given, so the CLI
   auto-selects only the latest snapshot, say `snap-789`. Read `issues.errors`, explain the
   problem — and proactively include a link, setting **both** bounds to that same resolved
   snapshot so it isn't left open-ended:
   `https://ice.example.com/table/graph?table=sales.orders&start_snapshot_id=snap-789&end_snapshot_id=snap-789&select_node_id=<file path of the erroring node>`
   so the user can look at the offending node directly, without needing to ask for it.
4. "What's the schema/properties/partition spec of `sales.orders`?"
   → `icegraph --base-url https://ice.example.com graph sales.orders` (no range needed — the current
   metadata is what matters here, and an unset range already defaults to the latest snapshot, say
   `snap-789`). Read the answer from the top-level `metadata` key, **not** any node's own fields:
   `metadata.schemas` for columns, `metadata.properties` for table properties,
   `metadata["partition-specs"]`/`metadata["default-spec-id"]` for the active partition spec,
   `metadata["format-version"]` for the table format version. Summarize the relevant fields rather
   than dumping the whole object back at the user, and proactively include a link to that same
   resolved snapshot (`select_node_id` doesn't apply on `/table/metadata`, so omit it):
   `https://ice.example.com/table/metadata?table=sales.orders&start_snapshot_id=snap-789&end_snapshot_id=snap-789`
5. "Which job wrote this snapshot's data?"
   → find the `snapshot`-type node for it in a `graph` result. If `action_link` is populated, give
   it to the user directly. Otherwise, use `summary["app-id"]` when `summary["engine-name"]` is
   `"spark"`, falling back to `summary["spark.app.id"]`. If either provides an ID, ask the user for
   an example Spark History Server URL (e.g. one they already have for another job), then give them
   that same URL with just the app-id swapped. If neither key provides an ID, say so rather than
   guessing at a job.
