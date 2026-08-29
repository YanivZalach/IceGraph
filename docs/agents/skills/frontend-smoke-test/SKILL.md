---
name: frontend-smoke-test
description: Runs a deterministic browser smoke test of the IceGraph frontend across Home, Snapshot Selection, Timeline, Metadata, FileTree, Graph, and Docs. Verifies full and bounded snapshot ranges, cross-view navigation, URL cleanup, keyboard shortcuts, console errors, and visible failures. Use when asked to smoke-test, QA, or check the frontend for regressions.
user-invocable: true
---

# Frontend smoke test

Test a real running IceGraph frontend in the fixed order below. Do not start or stop servers.

## Required inputs

Establish these before opening the browser:

1. Frontend URL.
2. Data mode: real backend, Docker demo, or MSW.
3. Test table, unless it will be selected from Browse catalog.

Ask only for missing inputs. Reuse values already established in the conversation. If the frontend
is not running, stop and wait for the user to start it.

## Test rules

- Use browser automation available in the current environment.
- Run tests in the order specified so later checks reuse the loaded graph.
- Wait for loading indicators to disappear before evaluating a page.
- After each page, inspect error-level console messages and visible error states.
- Record actual and expected behavior immediately when a check fails.
- Capture screenshots only for failures or ambiguous visual states.
- Do not treat a correct empty state as a failure.
- Close every extra tab opened by the test.

## Phase 1: Home and table selection

### Home

- The logo, table input, Continue action, and Browse catalog action render.
- Browse catalog loads tables and filtering works when available.
- Select the test table and continue to Snapshot Selection.

### Snapshot Selection

- Start and End snapshot lists load.
- Latest Metadata Only opens Metadata with `start_snapshot_id` and without `end_snapshot_id`.

Run both graph ranges:

1. **Full range:** Start is Full History and End is Latest. Generate Graph. Confirm neither snapshot
   parameter remains in the URL and Timeline contains the complete history.
2. **Bounded range:** choose non-default Start and End snapshots that form a strict subset. Generate
   Graph. Confirm both URL values and visibly fewer Timeline nodes than the full range.

Keep the bounded range loaded for the remaining table views. Confirm:

- Timeline begins at the selected Start snapshot.
- Metadata Current Snapshot equals the selected End snapshot.
- FileTree treats the selected End snapshot as the latest available snapshot in the bounded range.

A branch-write event may appear after the End timestamp when its diff explains another branch ref
moving. Do not report that documented case as a range failure.

## Phase 2: Table views

### Timeline

- Nodes and all legend types render.
- Selecting a node opens its details.
- Metadata and Snapshot actions, when present, open Graph in a new tab with the corresponding node
  selected and locked.
- Close the extra Graph tab after verification.

Keyboard checks:

- `h` or Left and `l` or Right move between snapshots, including first or last selection when none
  is selected.
- `j` and `k` scroll the open panel.
- `f` toggles panel fullscreen.
- `r` fits the timeline.
- Escape closes the panel.

### Metadata

- Structured metadata renders without an error state.
- Current Snapshot matches the bounded End snapshot.
- `j` and `k` scroll the page.

### FileTree

- Tree and Flat modes both render.
- Expand all, individual expansion, filters, and available scope controls respond.
- The snapshot selector reflects the bounded range.
- View in Graph opens a new tab with the selected file node.
- Close the extra Graph tab after verification.

### Graph

- Nodes and links render without an error state.
- Center Graph and Reset Full View respond.
- Press `i` to enter Lineage Traversal mode.
- Enter or Space selects Main Metadata and opens its panel.
- `h` or Left and `l` or Right traverse parents and children.
- When a traversal choice popup appears, select one displayed option and confirm traversal.
- `j` and `k` scroll the panel, `f` toggles fullscreen, and Escape closes it.
- `c` centers the graph and `r` resets it.

Canvas node clicks are not required. Keyboard traversal is the deterministic node-selection path.

## Phase 3: Cross-view state

- From each table view, press `1`, `2`, `3`, and `4` and confirm navigation to Timeline, Metadata,
  FileTree, and Graph.
- After arriving in Graph through a cross-tab selection, use a top navigation tab to leave Graph.
- Confirm `dup`, `cache_id`, and `select_node_id` do not leak into routes that do not own them.
- Confirm table and bounded snapshot parameters remain where required.

## Phase 4: Docs

- `/docs` loads without a table.
- Content and navigation render.
- `k` opens search.
- Control+n and Control+p change the selected result.
- Enter opens the result and Escape closes search.

## Result format

Report exactly these sections:

```text
Environment
- URL: ...
- Mode: ...
- Table: ...
- Range: ...

Results
- Home and catalog: PASS|FAIL
- Snapshot ranges: PASS|FAIL
- Timeline and keys: PASS|FAIL
- Metadata and keys: PASS|FAIL
- FileTree: PASS|FAIL
- Graph and keys: PASS|FAIL
- Cross-view state: PASS|FAIL
- Docs and keys: PASS|FAIL

Failures
- <page or flow>: expected ..., observed ...

Console errors
- <page>: <error>
```

Use `None` when Failures or Console errors is empty. Include screenshots only for listed failures.
