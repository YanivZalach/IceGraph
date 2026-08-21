# IceGraph

Read-only debugging and visualization of Apache Iceberg table metadata (Spark Connect, Iceberg v2). One context: the whole product speaks the language of Iceberg table history.

## Language

**Commit**:
One metadata file — a single change to the table's data or definition. An *event* is a commit as displayed; a *row* is pure UI.
_Avoid_: version, entry, change record

**Snapshot**:
An Iceberg snapshot — the state of the table's data produced by a commit that moved data. Not every commit creates one.
_Avoid_: revision

**Draft**:
A snapshot that no ref or current pointer has ever reached — written but never made visible. Iceberg's own docs call this a *staged* snapshot (write-audit-publish); the UI says "not published".
_Avoid_: staged, orphan, dangling

**Published**:
A draft that later became reachable — a ref or the current pointer moved onto it (or onto a cherry-picked copy of it).
_Avoid_: live (reserve "live" for the table's present state outside any window)

**Window**:
The loaded snapshot range picked in snapshot selection. Everything a page shows is scoped to it; claims that require seeing beyond it are forbidden.
_Avoid_: range, loaded range, view

**Re-point**:
A commit that moves `current-snapshot-id` to an existing snapshot without adding one — rollback, branch fast-forward, or an arbitrary switch.
_Avoid_: move, switch (as nouns)

**Current**:
The snapshot `current-snapshot-id` points at — what unqualified reads of the table see. In a historical window, "current" means current *as of window end*.
_Avoid_: head, latest
