# Browser graph cache

## Status

Accepted

## Context

Compiling an IceGraph graph can require several Spark operations. Reloading a page or opening the same graph URL in another tab previously repeated that work even when the relevant Iceberg metadata had not changed.

The project avoids additional services and treats Iceberg metadata as the source of truth. A server-side persistent graph store would add deployment and operational state.

## Decision

Store completed graph responses in IndexedDB, keyed by application version, table, and requested snapshot range.

Before restoring a graph, request the metadata file associated with the selected end snapshot. When the end snapshot is omitted, request the latest metadata file. Restore the cached graph only when that file matches the value extracted from its `main_metadata` node.

Keep at most 20 graphs. Evict least-recently-used entries and entries unused for more than 24 hours. Browser storage failures must never prevent graph compilation or rendering.

Normal refreshes and new tabs may restore validated data. Recompile graph and keyboard hard refresh explicitly bypass restoration.

## Consequences

- No new backend service or persistent server state is required.
- Cache validation adds one lightweight backend request.
- A cache miss or validation failure falls back to normal graph compilation.
- Application version changes isolate incompatible cache records.
- Bounded historical graphs can remain stale if referenced files are removed without changing the pinned metadata file.
- Backend collection-setting changes require an application version change or an explicit recompile to invalidate existing graphs.
