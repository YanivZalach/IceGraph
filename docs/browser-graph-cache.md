# Browser graph cache

## Status

Accepted

## Context

Compiling an IceGraph graph can require several Spark operations. Reloading a page or opening the same graph URL in another tab previously repeated that work even when the relevant Iceberg metadata had not changed.

The project avoids additional services and treats Iceberg metadata as the source of truth. A server-side persistent graph store would add deployment and operational state.

## Decision

Store completed graph responses in IndexedDB, keyed by application version, table, and requested snapshot range.

Before restoring a graph, request the metadata file associated with the selected end snapshot: the newest metadata file in the range graph collection would use. That range is worked out from the current metadata file: it starts at the first metadata file written when the snapshot was committed and ends before the last snapshot built on top of it was committed, on any branch. A snapshot that nothing was built on yet stays open up to the newest file while a branch points at it, and a snapshot left behind by a dropped or reset branch ends before the next snapshot committed anywhere in the table. Branches and tags never extend a range. Ranges ending on a non-main branch validate the same way as ranges on main. When the end snapshot is omitted, request the latest metadata file. Restore the cached graph only when that file matches the value extracted from its `main_metadata` node.

Keep at most 20 graphs. Evict least-recently-used entries and entries unused for more than 24 hours. Browser storage failures must never prevent graph compilation or rendering.

Normal refreshes and new tabs may restore validated data. Recompile graph explicitly bypasses restoration.

## Consequences

- No new backend service or persistent server state is required.
- Cache validation adds one lightweight backend request.
- A cache miss or validation failure falls back to normal graph compilation. A validation failure is shown on the graph loading screen with its reason.
- Application version changes isolate incompatible cache records.
- Bounded historical graphs can remain stale if referenced files are removed without changing the pinned metadata file.
- Backend collection-setting changes require an application version change or an explicit recompile to invalidate existing graphs.
- The Graph view's Current metadata node is not part of the cached graph. It is requested on every Graph view open, so a restored graph still shows the table's current metadata file.
