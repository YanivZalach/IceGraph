---
name: frontend-smoke-test
description: Runs a deterministic browser smoke test of IceGraph across table selection, snapshot ranges, Timeline, Metadata, FileTree, Graph, Docs, URL cleanup, browser caching, keyboard controls, and console errors. Supports full testing with a real backend or Docker demo and a reduced MSW profile. Use when asked to smoke-test, QA, or check frontend regressions.
user-invocable: true
---

# Frontend smoke test

Test an already running IceGraph frontend. Never start or stop a server.

## Preconditions

Establish and report:

1. Frontend URL and whether it is reachable.
2. Profile: `full` for a real backend or Docker demo, or `msw` for mock mode.
3. Test table. In MSW use `default.events`; in full mode it may be selected through Browse catalog.
4. Browser capabilities: navigation, keyboard input, page inspection, tabs, screenshots, and console
   inspection.

Ask only for missing information. If the URL is unreachable, stop and wait for the user. If a
browser capability is unavailable, mark only its dependent checks `NOT RUN`; never silently pass
them.

## Profiles

| Check | Full | MSW |
| --- | --- | --- |
| Home and catalog | Required | N/A, redirects to Timeline |
| Snapshot Selection | Required | N/A, redirects to Timeline |
| Full-range graph | Required | Required with fixed mock graph |
| Bounded-range rendered data | Required | N/A, mock serves one fixed graph |
| Table views and keyboard controls | Required | Required |
| Cache restoration and Recompile | Required | N/A |
| Docs | Required | Required |

Use `PASS`, `FAIL`, `N/A`, `BLOCKED`, or `NOT RUN` for every result.

## Test rules

- Use browser automation available in the current environment.
- Follow the phases in order.
- Wait up to 120 seconds for each graph load, polling loading state rather than sleeping once.
- Inspect visible error states and error-level console messages after every page.
- Record expected and observed behavior immediately after a failure.
- Capture screenshots only for failures or ambiguous visual states.
- Treat valid empty and mode-limited states as `N/A`, not failures.
- Preserve full snapshot IDs digit-for-digit. Never compare them as JavaScript numbers.
- Close extra tabs and remove temporary screenshots before finishing.

## Phase 1: Entry and ranges

### Full profile

Home:

- Logo, table input, Continue, and Browse catalog render.
- Browse catalog loads and filtering works.
- Select a table with at least three ordered snapshots. If unavailable, mark bounded-range checks
  `BLOCKED` and explain why.

Snapshot Selection:

- Start and End lists load.
- Latest Metadata Only opens Metadata with `start_snapshot_id` and `end_snapshot_id` both equal to
  the latest snapshot ID. Current Snapshot matches that exact ID.
- Full range: select Full History and Latest, then Generate Graph. Neither snapshot parameter remains
  in the URL and Timeline renders the loaded history.
- Bounded range: select non-default Start and End values forming a strict subset, then Generate
  Graph. Both URL values match the selected IDs digit-for-digit, including IDs above `2^53`.
- The bounded Timeline visibly contains fewer snapshot nodes than the full range and begins at the
  selected Start snapshot.
- Metadata Current Snapshot equals the selected End snapshot.
- FileTree treats the selected End snapshot as the latest available snapshot in the bounded range.

A branch-write event may appear after the End timestamp when its diff explains another branch ref
moving. That documented case is not a range failure.

Reload the bounded URL and confirm Timeline, Metadata, and FileTree still represent the same range.
Use browser Back and Forward once and confirm the restored route and range match their URLs.

### MSW profile

- Open the configured URL and confirm it redirects to Timeline for `default.events`.
- Confirm the fixed mock graph loads.
- Mark Home, catalog, Snapshot Selection, and bounded-range rendered-data checks `N/A`.

## Phase 2: Table views

Keep the bounded range loaded in full mode and the fixed graph loaded in MSW mode.

### Timeline

- Timeline nodes and the legend render.
- Selecting a node opens its details.
- Metadata and Snapshot actions, when present, open Graph in a new tab with the intended node
  selected and locked. Close the extra tab afterward.
- `h` or Left and `l` or Right move between snapshots, including selecting the first or last node
  when none is selected.
- `j` and `k` scroll the panel, `f` toggles fullscreen, `r` fits the timeline, and Escape closes the
  panel.

### Metadata

- Structured metadata renders without an error state.
- In full mode Current Snapshot matches the bounded End snapshot.
- `j` and `k` scroll the page.

### FileTree

- Tree and Flat modes render.
- Expansion, search, filters, and available scope controls respond.
- In full mode the snapshot selector reflects the bounded range.
- View in Graph opens a new tab with the selected file node. Close the extra tab afterward.
- Set at least one non-default `filetree_*` option for the cleanup check in Phase 3.

### Graph

- Nodes and links render without an error state.
- Center Graph and Reset Full View respond.
- Ensure the mode control reads Lineage Traversal. Press `i` only if it currently reads Inspect
  (Locked).
- Enter or Space selects Main Metadata and opens its panel.
- `h` or Left and `l` or Right traverse parents and children.
- If a traversal choice popup appears, choose one displayed option and confirm traversal.
- `j` and `k` scroll the panel, `f` toggles fullscreen, Escape closes it, `c` centers the graph, and
  `r` resets it.

Canvas node clicks are not required. Keyboard traversal is the deterministic selection path.

### Cache, full profile only

- Reload the graph route and confirm a validated graph can restore from browser cache.
- Use Recompile graph and confirm collection progress appears and a fresh graph replaces the cached
  result.
- Confirm the final graph renders and no stale selection remains.

## Phase 3: Cross-view state

- Use one continuous keyboard sequence: Timeline `2` to Metadata, `3` to FileTree, `4` to Graph, and
  `1` back to Timeline.
- After a Graph deep-link selection is applied, confirm `select_node_id` is removed while table and
  snapshot-range parameters remain.
- Navigate away from FileTree and confirm all `filetree_*` parameters are removed from Timeline,
  Metadata, and Graph.
- Confirm closing selections, resetting controls, and leaving each view do not affect the next view.
- Confirm no extra browser tabs remain.

## Phase 4: Docs

- `/docs` loads without a table and renders content and navigation.
- `k` opens search.
- Control+n and Control+p change the selected result.
- Enter opens the result and Escape closes search.

## Report

Use exactly this structure:

```text
Environment
- URL: ...
- Reachable: yes|no
- Profile: full|msw
- Table: ...
- Range: ...|N/A
- Capabilities: navigation ..., keyboard ..., inspection ..., tabs ..., screenshots ..., console ...

Results
- Home and catalog: PASS|FAIL|N/A|BLOCKED|NOT RUN
- Snapshot ranges and precision: PASS|FAIL|N/A|BLOCKED|NOT RUN
- Reload, history, and cache: PASS|FAIL|N/A|BLOCKED|NOT RUN
- Timeline and keys: PASS|FAIL|N/A|BLOCKED|NOT RUN
- Metadata and keys: PASS|FAIL|N/A|BLOCKED|NOT RUN
- FileTree: PASS|FAIL|N/A|BLOCKED|NOT RUN
- Graph and keys: PASS|FAIL|N/A|BLOCKED|NOT RUN
- Cross-view state: PASS|FAIL|N/A|BLOCKED|NOT RUN
- Docs and keys: PASS|FAIL|N/A|BLOCKED|NOT RUN

Failures
- <page or flow>: expected ..., observed ...

Console errors
- <page>: <error>

Not run or limited
- <check>: <reason>

Cleanup
- Extra tabs: 0
- Temporary artifacts: 0
```

Use `None` for empty Failures, Console errors, or Not run or limited sections. Include screenshots
only for listed failures.
