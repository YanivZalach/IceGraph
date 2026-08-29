---
name: frontend-smoke-test
description: Smoke-tests the IceGraph frontend by driving the running app with Playwright across all pages and flows (Home, Snapshot Selection, Timeline, Metadata, FileTree, Graph, Docs), exercising keyboard shortcuts on every page, checking for console errors and broken states, and reporting a pass/fail summary. Never starts its own dev server — always asks the user first whether one is already running and at what URL. Use when asked to test, smoke-test, or QA the frontend, verify nothing is broken across pages, or check a change didn't regress other pages.
user-invocable: true
---

# frontend-smoke-test

Drives the real, running IceGraph frontend with Playwright browser tools and checks that every
page loads cleanly and the core interactions work. This is a manual-style smoke test (no test
framework involved) — it reproduces the kind of click-through QA a person would do, but scripted
and repeatable.

Optional argument: a table name to test against (e.g. `default.events`). If omitted, ask for one
in step 2.

## Step 1 — find the dev server (always ask first)

**Never start a dev server yourself.** Before doing anything else, ask the user with
`AskUserQuestion`:

- "Yes — running at http://localhost:3000" (the default Vite port for this repo)
- "Yes — different URL" (they'll specify via Other)
- "No — I'll start it myself and tell you when it's ready"

If they say it's not running, stop and wait for them to confirm it's up rather than launching it
yourself (per `! pnpm run dev` guidance, that's their call to make, not yours). Skip re-asking if
the dev server / backend location was already established earlier in the same conversation —
don't make the user repeat themselves.

Also confirm the backend is reachable the same way if you don't already know: ask whether a
backend is running (Flask on port 5050 by default, proxied through Vite) or whether they want you
to use the MSW-mocked demo data instead. Don't assume — this repo can run against a real Spark
Connect backend, a docker demo stack, or the `VITE_USE_MSW=true` mock mode, and each has
different available tables.

If deferred Playwright tools (`mcp__plugin_playwright_playwright__browser_*`) aren't loaded yet in
this session, fetch them with `ToolSearch` before proceeding (query: `"playwright browser"`).

## Step 2 — pick a table

If not passed as an argument, ask the user which table to test against, or use
`mcp__plugin_playwright_playwright__browser_navigate` to the dev server root and use the
**Browse catalog** picker on the Home page to find one. Note the table name — every table-scoped
page below needs it as a `?table=` query param.

## Step 3 — walk every page

For each page below: navigate, wait for loading states to clear
(`mcp__plugin_playwright_playwright__browser_wait_for` on "Loading" text going away, where
applicable), call `mcp__plugin_playwright_playwright__browser_console_messages` and note any
`error`-level entries, and take a screenshot only if something looks broken (empty state when data
was expected, a visible error banner, obviously broken layout). Don't screenshot happy-path pages
— keep the report signal-heavy.

1. **Home (`/`)** — page loads, title/logo render, table-name input and Continue button present.
   Test **Browse catalog**: click it, confirm the table list loads (or the filter field works if
   many tables).
2. **Snapshot Selection (`/snapshots-selection?table=<table>`)** — reachable from Home's Continue
   button. Confirm the Start/End snapshot lists populate. Click **Latest Metadata Only** (to the
   left of the pickers) and confirm it lands on `/table/metadata` with a `start_snapshot_id` set
   and no `end_snapshot_id`. Then run these two range variations as separate tests — each one
   should reach `/table/timeline` via **Generate Graph** and actually render a different set of
   nodes, which is the real proof the range param round-trips correctly, not just that the page
   didn't error:
   - **Full range**: select `-- Full History --` for Start and `-- Latest --` for End, then
     Generate Graph. Confirm the resulting `start_snapshot_id`/`end_snapshot_id` are both absent
     from the URL and the Timeline shows the table's entire snapshot history (the `Init` node
     plus every later event).
   - **Bounded range**: go back to Snapshot Selection and pick a specific, non-default Start
     snapshot and a specific, non-default End snapshot (not the first/last in the list — something
     in the middle so the range is a strict subset). Generate Graph, confirm both
     `start_snapshot_id` and `end_snapshot_id` are present and correct in the URL, and confirm the
     Timeline shows only that bounded window of snapshots (visibly fewer nodes than the full-range
     run, first node matching your chosen Start). Cross-check the range actually applied
     everywhere, not just on Timeline: the Metadata page's **Current Snapshot** field should equal
     your chosen End, and FileTree's snapshot dropdown should show your End as the "latest" entry
     in that bounded list. Note: the timeline's *last visible node* can legitimately show a
     timestamp slightly after your chosen End if it's a Branch Write event recording another
     branch's ref moving past the range — that's correct domain behavior, not a bug; only flag it
     if the diff panel doesn't explain it.

   Keep the *rest* of the Timeline/Metadata/FileTree/Graph checklist below (steps 3–6) to a single
   pass — do it on whichever of the two range loads is already open rather than repeating the full
   node-interaction checklist twice; the range variations above are specifically what verifies the
   range parameter itself, not the per-page interactions.
3. **Timeline (`/table/timeline?table=<table>`)** — nodes render along the timeline, legend shows
   all 4 types. Click a node to open the side panel; confirm the **Metadata** button (and
   **Snapshot** button, when the node has one) work — clicking should open a **new tab** on
   `/table/graph` with the corresponding node selected and locked (verify via a screenshot in that
   new tab), then close that tab. Exercise the documented keyboard shortcuts (see **Keybindings**
   below) — don't just spot-check `Esc`.
4. **Metadata (`/table/metadata?table=<table>`)** — structured metadata view renders without
   errors. Exercise its keybindings (`j`/`k` scroll).
5. **FileTree (`/table/filetree?table=<table>`)** — expand a folder (or use **Expand all**), click
   the small graph-icon **View in graph** button on a file row, confirm it opens a new tab on
   `/table/graph` with that file node selected, then close that tab. Toggle **Flat**/**Tree** view
   and a few checkboxes.
6. **Graph (`/table/graph?table=<table>`)** — the force-graph renders nodes without errors. Try
   **Reset Full View** and **Center Graph**. For node interaction, use the **keyboard method**
   below rather than clicking the canvas directly (see **Testing the Graph page** — direct canvas
   clicks don't work in this automation setup). Exercise its keybindings.
7. **Docs (`/docs`)** — loads standalone (no table needed), content renders. Exercise its
   keybindings (`k` search overlay, `Ctrl+n`/`Ctrl+p`, `Enter`, `Esc`).

After steps 3–4's cross-tab checks, also do the reverse sanity check this branch's work depends
on: from a Graph tab reached via a cross-tab select, click a top-nav tab (e.g. Timeline) and
confirm the URL stays clean (no leftover `dup`, `cache_id`, or `select_node_id` query params) and
the page still loads — these params are known to have caused subtle regressions before, so treat
any leftover params as a bug worth reporting even if the page doesn't visibly break.

### Testing the Graph page

Direct clicks on the force-graph canvas **do not work** with this Playwright MCP setup — neither
`browser_click` (no DOM element to target; nodes are canvas-rendered) nor a synthetic
`element.dispatchEvent(new MouseEvent(...))` via `browser_evaluate` (react-force-graph-2d appears
to require trusted pointer events, which script-dispatched events aren't). Don't waste time on
canvas coordinate clicking.

Instead, use the fully keyboard-driven path, which is documented app behavior and verified working:

1. The Graph page defaults to **Inspect Mode ON**, which *disables* keyboard node navigation (by
   design, so free mouse interaction doesn't fight with keyboard focus). Press `i` first to switch
   to **Lineage Traversal** mode (toolbar button label changes to confirm).
2. Press `Enter` or `Space` to jump to and select the **Main Metadata** node — this opens the side
   panel, no click needed.
3. Press `l`/`→` to navigate to children, `h`/`←` for parents. If a node has more than one
   parent/child, a **combo picker popup** appears ("Type combo to select") — press the shown
   letter key (e.g. `a`, `b`) to pick one.
4. Press `f` to toggle the panel fullscreen, `j`/`k` to scroll it, `Esc` to close it.
5. Press `c` to center/fit the whole graph, `r` to reset the view.

This exercises node selection, the side panel, and lineage traversal end-to-end without ever
needing a working canvas click — and it doubles as the Graph page's keybinding test.

### Keybindings — exercise these on every relevant page

Pulled from the app's own Docs page (`Keyboard Shortcuts` section) — check this list against that
page if it's been a while, since it can drift as features are added:

- **Global**: `1`/`2`/`3`/`4` jump to Timeline/Metadata/FileTree/Graph respectively.
- **Docs page**: `k` opens the search overlay, `Ctrl+n`/`Ctrl+p` move through results, `Enter`
  opens the selected result, `Esc` closes the overlay.
- **Timeline**: `Shift+Scroll` pans horizontally, `r` fits the whole timeline, `h`/`←` and `l`/`→`
  step to the previous/next snapshot (jumping to first/last if none is selected — verify this
  jump-when-none-selected behavior specifically, it's easy to regress), `j`/`k` scroll the open
  side panel, `f` toggles its fullscreen, `Esc` closes it.
- **Metadata**: `j`/`k` scroll the page.
- **Graph**: see **Testing the Graph page** above — `i` toggles Inspect Mode, `c` fits the graph,
  `r` resets the view, `Enter`/`Space` jumps to Main Metadata, `h`/`l` traverse parent/child
  (with the combo-picker popup on multi-parent/child nodes), `j`/`k` scroll the panel, `f`
  fullscreen, `Esc` closes it.

A shortcut silently doing nothing (no visible state change, no console error) is just as much a
bug as a crash — call it out in the report.

## Step 4 — report

Give a compact summary, not a wall of narration:

- A pass/fail line per page/flow from Step 3, plus a line per page noting whether its keybindings
  were exercised and any that didn't do what they should.
- Any console errors found, with the page they occurred on.
- Any screenshots taken of broken states — send them with `SendUserFile` if there are issues to
  show, otherwise don't bother generating images for a clean run.
- Any leftover-query-param or dead-tab issues from the cross-tab check.

## Cleanup

Close any extra browser tabs you opened during the run, and delete any screenshot files you wrote
to the repo root or scratchpad once they've been reported (via `SendUserFile` or inline
description) — don't leave test artifacts lying around in the working tree.

## Tips learned from prior runs

- **Loading detection**: wait for the "Loading data" text to disappear
  (`browser_wait_for` with `textGone`) before asserting on a table-scoped page — the graph job is
  async and a screenshot taken too early just shows the spinner.
- **Disambiguating snapshot rows**: Start and End snapshot lists both render every snapshot's ID
  as text, so a plain `text=ID: <id>` locator matches twice (once per column) and Playwright's
  strict mode will error. Use `.first()`/`nth(0)` for the Start column and `nth(1)` for the End
  column (Start renders before End in the DOM), or scope by a unique nearby ancestor.
- **Cross-tab flow checks**: after clicking a Metadata/Snapshot/View-in-graph button, the new tab
  needs `browser_tabs` `select` to switch into it, then a short wait (~1.5s is enough) before the
  cached data has loaded and the node selection has resolved — screenshotting immediately can
  catch it mid-load. Always close the extra tab afterward.
- **Verifying a range actually applied**: don't just check the URL params — cross-check the
  *rendered data*. Compare node counts between a full-range and bounded-range load, check the
  Metadata page's Current Snapshot field, and check the FileTree snapshot dropdown's "latest"
  label. Matching URL params with unchanged rendered data would mean the param is silently
  ignored, which the URL check alone can't catch.
