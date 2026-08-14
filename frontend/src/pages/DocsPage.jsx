import { useState, useRef, useEffect, cloneElement, Fragment } from "react";
import { UI_DOCS_BODY_CLASS, UI_DOCS_NAV_TITLE_CLASS } from "../uiTypography";
import { APP_VERSION, BASE_PATH } from "../appConstants";

const PIP_INSTALL_COMMAND =
  APP_VERSION === "dev"
    ? "pip install icegraph-client"
    : `pip install icegraph-client==${APP_VERSION.replace(/^v/, "")}`;

function Key({ k }) {
  return (
    <kbd className="bg-surface-hover border border-[#3d4a5c] text-[#7dd3fc] text-xs font-mono px-2 py-0.5 rounded">
      {k}
    </kbd>
  );
}

function ShortcutRow({ keys, desc }) {
  return (
    <div className="flex items-center gap-3 py-1.5 border-b border-[#1e2736]">
      <div className="flex items-center gap-1 shrink-0 min-w-[6rem]">
        {keys.map((k, i) => (
          <span key={i} className="flex items-center gap-1">
            {i > 0 && <span className="text-slate-600 text-xs">/</span>}
            <Key k={k} />
          </span>
        ))}
      </div>
      <span className="text-slate-300 text-sm">{desc}</span>
    </div>
  );
}

const TEXT_PROP_BY_TYPE = new Map([
  [ShortcutRow, "desc"],
  [Key, "k"],
]);

function escapeRegExp(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function findAllIndices(text, query) {
  const indices = [];
  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();

  let from = 0;
  while (true) {
    const index = lowerText.indexOf(lowerQuery, from);
    if (index === -1) break;
    indices.push(index);
    from = index + lowerQuery.length;
  }

  return indices;
}

function highlightMatch(text, query) {
  if (!query) return text;

  const regex = new RegExp(`(${escapeRegExp(query)})`, "gi");

  return text.split(regex).map((part, index) => {
    if (part.toLowerCase() === query.toLowerCase()) {
      return (
        <mark key={index} className="bg-yellow-400 text-black px-1 rounded">
          {part}
        </mark>
      );
    }

    return part;
  });
}

function highlightTreeMatches(node, query, matchState, activeMarkRef) {
  if (node == null || typeof node === "boolean") return node;

  if (typeof node === "string") {
    if (!node.toLowerCase().includes(query.toLowerCase())) return node;

    const regex = new RegExp(`(${escapeRegExp(query)})`, "gi");

    return node.split(regex).map((part) => {
      if (part.toLowerCase() !== query.toLowerCase()) return part;

      const isActive = matchState.count === matchState.target;
      matchState.count += 1;

      return (
        <mark
          key={`match-${matchState.key++}`}
          ref={isActive ? activeMarkRef : undefined}
          className={
            isActive
              ? "bg-accent text-white px-1 rounded ring-2 ring-white"
              : "bg-yellow-400 text-black px-1 rounded"
          }
        >
          {part}
        </mark>
      );
    });
  }

  if (Array.isArray(node)) {
    return node.map((child, i) => (
      <Fragment key={i}>
        {highlightTreeMatches(child, query, matchState, activeMarkRef)}
      </Fragment>
    ));
  }

  const textProp = TEXT_PROP_BY_TYPE.get(node.type);
  if (textProp) {
    return cloneElement(node, {
      [textProp]: highlightTreeMatches(
        node.props[textProp],
        query,
        matchState,
        activeMarkRef,
      ),
    });
  }

  if (node.props?.children) {
    return cloneElement(node, {
      children: highlightTreeMatches(
        node.props.children,
        query,
        matchState,
        activeMarkRef,
      ),
    });
  }

  return node;
}

const SECTIONS = [
  {
    id: "overview",
    title: "Overview",
    body: (
      <div className="space-y-4">
        <p>
          IceGraph is an open source Apache Iceberg debugging and visualization
          platform. Trace production Iceberg tables through a graph based UI
          built for debugging complex metadata states, analyzing table
          evolution, and learning how Iceberg works under the hood.
        </p>
        <p>
          Everything is <strong className="text-white">read-only</strong>.
        </p>
        <div className="border-t border-edge pt-4 flex flex-col gap-2 text-xs">
          <div className="flex items-center justify-between text-slate-400">
            <span className="text-slate-500 uppercase tracking-wider text-tiny font-semibold">
              Version
            </span>
            <span className="font-mono text-slate-300">{APP_VERSION}</span>
          </div>
          <div className="flex items-center justify-between text-slate-400">
            <span className="text-slate-500 uppercase tracking-wider text-tiny font-semibold">
              Source
            </span>
            <a
              href="https://github.com/YanivZalach/IceGraph"
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent hover:text-blue-400 transition font-mono"
            >
              github.com/YanivZalach/IceGraph
            </a>
          </div>
        </div>
      </div>
    ),
  },
  {
    id: "claude-code",
    title: "Connect to Coding Agent",
    body: (
      <div className="space-y-5">
        <p>
          IceGraph ships an AI-assistant{" "}
          <strong className="text-white">skill</strong> so you can ask an agent
          to inspect and debug your tables directly - list tables, read snapshot
          history, pull the metadata graph, and get links back into this UI, all
          in plain language.
        </p>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Claude Code</h3>
          <p>Ships as a plugin. Inside Claude Code, run:</p>
          <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap">{`/plugin marketplace add YanivZalach/IceGraph
/plugin install icegraph`}</pre>
          <p>
            This is a <strong className="text-white">one-time install</strong> -
            once it's done, the plugin is available in every future Claude Code
            session on that machine, not just the current one.
          </p>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">
            Other coding agents / Manual install
          </h3>
          <p>
            No plugin system needed - the skill is a single, self-contained
            instructions file. Grab it, then tell your agent to read and follow
            it:
          </p>
          <a
            href={`${BASE_PATH}/SKILL.md`}
            download="SKILL.md"
            target="_blank"
            rel="noopener noreferrer"
            className="inline-block text-accent hover:text-blue-400 transition font-mono text-sm underline"
          >
            Download SKILL.md
          </a>
          <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap">{`Read the SKILL.md file I just downloaded and follow it as your instructions whenever you work with Iceberg tables and IceGraph.`}</pre>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Requirements</h3>
          <p>
            Nothing to set up ahead of time - the agent checks for{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              icegraph-client
            </code>{" "}
            itself, gives you the right install command if it's missing, and
            asks for your server's address and any auth it needs as you go.
          </p>
        </div>
      </div>
    ),
  },
  {
    id: "loading-a-table",
    title: "Loading a Table",
    body: (
      <div className="space-y-5">
        <div className="space-y-2">
          <h3 className="text-white font-semibold">1. Enter the table name</h3>
          <p>
            From the Home page, type the fully-qualified name of your Iceberg
            table (e.g.{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              database.table_name
            </code>
            ) and press Enter or click Continue. You can also click{" "}
            <strong className="text-white">Browse catalog</strong> to list
            Iceberg tables from the Spark catalog. Use the filter field to
            narrow the list when many tables are available. When the non-Iceberg
            catalogs are included, the list notes that non-Iceberg tables may
            also appear.
          </p>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">2. Pick a snapshot range</h3>
          <p>
            IceGraph shows you the table's snapshot history. Select the range of
            snapshots you want to explore. A smaller range loads faster and
            produces a less cluttered graph. If you just want the table's
            current state, click{" "}
            <strong className="text-white">Latest Metadata Only</strong> to the
            left of the snapshot pickers to skip range selection and go straight
            to the Metadata view for the latest snapshot.
          </p>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">3. Wait for the graph</h3>
          <p>
            IceGraph fetches the metadata in the background. Once ready, you
            land on the Timeline view. Large ranges with many data files may
            take a moment.
          </p>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Switching tables</h3>
          <p>
            While viewing a table, click the table name in the navbar to change
            tables. Enter a new table or use{" "}
            <strong className="text-white">Browse catalog</strong>, then click
            Continue. IceGraph opens the new table in a separate browser tab so
            your current graph stays loaded.
          </p>
        </div>
      </div>
    ),
  },
  {
    id: "timeline-view",
    title: "Timeline View",
    body: (
      <div className="space-y-4">
        <p>
          A chronological list of every snapshot in your selected range. Each
          row shows when the snapshot was created, what operation produced it,
          and how many files and records changed.
        </p>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Operation types</h3>
          <ul className="list-disc list-inside space-y-2">
            <li>
              <strong className="text-white">append</strong> — new data was
              added to the table
            </li>
            <li>
              <strong className="text-white">overwrite</strong> — new data was
              written, and any existing data in the affected partitions was
              replaced
            </li>
            <li>
              <strong className="text-white">replace</strong> — files were
              rewritten without changing the actual records (compaction,
              rewriting manifests, etc)
            </li>
            <li>
              <strong className="text-white">delete</strong> — rows or files
              were removed from the table
            </li>
          </ul>
        </div>
        <p>
          Use the Timeline to pinpoint when a large write happened, spot
          unexpected deletes, or verify that a compaction job ran as expected.
        </p>
        <p>
          A red <strong className="text-white">Unknown Events</strong> marker
          appears when metadata or snapshot data is missing. It indicates that
          one or more events occurred in that part of the timeline, even when
          the exact events cannot be determined.
        </p>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Zoom &amp; pan</h3>
          <p>
            Scroll the mouse wheel to zoom in and out (text and nodes scale
            together, like Graph view). Drag the timeline background to pan. Use
            horizontal trackpad scroll or Shift + wheel to pan sideways without
            zooming. <strong className="text-white">Fit Timeline</strong> scales
            the full history to the viewport and centers it.
          </p>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Details panel</h3>
          <p>
            Click a timeline event to open its details in a panel on the right —
            the same panel used in Graph view. Drag the left-edge grip to widen
            it, use fullscreen to expand, and copy field values with the
            clipboard icon. Long JSON diffs are collapsed by default and can be
            expanded or collapsed with <strong className="text-white">▼</strong>{" "}
            / <strong className="text-white">▲</strong>.
          </p>
        </div>
      </div>
    ),
  },
  {
    id: "metadata-view",
    title: "Metadata View",
    body: (
      <div className="space-y-4">
        <p>
          Shows the structured metadata of your table — schema, partition spec,
          and sort order. Use this to verify column types, understand partition
          strategies, and inspect how the schema has evolved.
        </p>
        <ul className="list-disc list-inside space-y-1">
          <li>
            Column IDs are stable even when columns are renamed — useful for
            tracing schema evolution
          </li>
          <li>
            <strong className="text-white">Overview</strong> fields include a
            clipboard icon to copy individual values
          </li>
        </ul>
      </div>
    ),
  },
  {
    id: "filetree-view",
    title: "FileTree View",
    body: (
      <div className="space-y-4">
        <p>
          Shows all data files in your selected snapshot range organized as a
          directory tree, grouped by their partition paths.
        </p>
        <p>
          This view solves a common source of confusion: if you look at the{" "}
          <strong className="text-white">raw storage directory</strong> written
          by your engine (Spark, for example), you see all files ever written —
          including files from old snapshots that have since been replaced, and
          files that belong to different table versions.
        </p>
        <p>
          <strong className="text-white">
            What's on disk is not the same as what Iceberg considers the current
            table.
          </strong>{" "}
          The FileTree view shows only the files Iceberg actually tracks as part
          of the selected snapshots, giving you a true picture of the table's
          data.
        </p>
        <ul className="list-disc list-inside space-y-1">
          <li>Expand directories to see individual files</li>
          <li>Choose a branch, and within it, a snapshot to explore</li>
          <li>
            If the selected snapshot or one of its included files could not be
            read, an error notice identifies the file and explains why
          </li>
          <li>
            Many small files in one partition path often indicates a small-file
            problem
          </li>
          <li>
            Each file shows its{" "}
            <strong className="text-white">first appearing timestamp</strong>{" "}
            tracked by Iceberg in the{" "}
            <strong className="text-white">asked snapshot range</strong>
          </li>
          <li>
            Each folder shows a{" "}
            <strong className="text-white">last modified</strong> timestamp —
            the most recent first-appearing timestamp among all its files
          </li>
        </ul>
      </div>
    ),
  },
  {
    id: "graph-view",
    title: "Graph View",
    body: (
      <div className="space-y-5">
        <p>
          The Graph view shows all Iceberg metadata objects in your selected
          range as a directed acyclic graph. Each node is a file. Links show
          parent→child relationships.
        </p>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Node types</h3>
          <ul className="list-disc list-inside space-y-2">
            <li>
              <strong className="text-white">Metadata file</strong> — JSON file
              describing the current full state of the table, combining schema,
              partition spec, snapshot history, and data file references
            </li>
            <li>
              <strong className="text-white">Snapshot</strong> — Avro file that
              represents a point-in-time version of the table produced by a data
              operation (append, overwrite, etc.)
            </li>
            <li>
              <strong className="text-white">Manifest</strong> — tracks which
              data files exist and stores per-file statistics
            </li>
            <li>
              <strong className="text-white">Data file</strong> — the actual
              Parquet, ORC, or Avro file containing your rows. IceGraph is not
              reading the data file: all of the data shown comes from the
              manifest entries that point at it
            </li>
            <li>
              <strong className="text-white">Unreadable file</strong> — a file
              whose metadata could not be obtained is drawn in red and shows the
              reason in its details panel. The rest of the graph still loads
            </li>
          </ul>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Interactions</h3>
          <ul className="list-disc list-inside space-y-1">
            <li>
              Click a node to select it and open its details in the side panel
            </li>
            <li>Drag a node to reposition it</li>
            <li>Scroll to zoom, drag the background to pan</li>
          </ul>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Details panel</h3>
          <p>
            The panel on the right lists every metadata field for the selected
            node. The header shows the file type, path, and timestamp; fields
            below use the same layout as Timeline and other views.
          </p>
          <ul className="list-disc list-inside space-y-1">
            <li>
              <strong className="text-white">Resize</strong> — drag the grip
              handle on the left edge of the panel to widen it. Wider panels
              give text fields more room and show more lines before you need to
              expand a field.
            </li>
            <li>
              <strong className="text-white">Fullscreen</strong> — click the
              expand button in the panel header to fill the graph area. Click
              the compress button or press{" "}
              <strong className="text-white">Esc</strong> to exit.
            </li>
            <li>
              <strong className="text-white">Copy</strong> — click the clipboard
              icon inside any field to copy its value.
            </li>
            <li>
              <strong className="text-white">Long values</strong> — fields with
              many lines can be expanded or collapsed individually with{" "}
              <strong className="text-white">▼</strong> /{" "}
              <strong className="text-white">▲</strong>.
            </li>
          </ul>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Reading the graph</h3>
          <p>
            Nodes shared across multiple snapshots mean Iceberg reused those
            files — data that didn't change is never rewritten. Seeing many
            shared data files between snapshots is normal and efficient. A
            snapshot with no shared manifests or data files means a full
            overwrite occurred.
          </p>
        </div>
      </div>
    ),
  },
  {
    id: "specs-panel",
    title: "Specs Panel",
    body: (
      <div className="space-y-4">
        <p>
          The <strong className="text-white">Specs</strong> button in the navbar
          opens the Table Specification panel, which shows the full history of
          your table's structural definitions across three sections:
        </p>
        <ul className="list-disc list-inside space-y-2">
          <li>
            <strong className="text-white">Schema History</strong> — every
            schema version the table has had, showing each by its schema ID
          </li>
          <li>
            <strong className="text-white">Partition History</strong> — every
            partition spec version, showing each by its spec ID
          </li>
          <li>
            <strong className="text-white">Order History</strong> — every sort
            order version, showing each by its order ID
          </li>
        </ul>
        <p>
          The currently active version in each section is highlighted with an{" "}
          <strong className="text-white">ACTIVE</strong> badge. Click any
          version to expand its full field-level definition — schema fields
          render with their types (including nested structs, lists, and maps),
          while partition and sort order fields render as structured tables. Use
          the <strong className="text-white">Full</strong> /{" "}
          <strong className="text-white">Diff</strong> toggle to compare a
          version against the one before it, with added, removed, and changed
          fields highlighted.
        </p>
      </div>
    ),
  },
  {
    id: "issues-panel",
    title: "Issues Panel",
    body: (
      <div className="space-y-4">
        <p>
          When the backend reports problems during metadata collection, an{" "}
          <strong className="text-white">Issues</strong> button appears in the
          navbar. There are two severity levels:
        </p>
        <div className="space-y-3">
          <div className="space-y-1">
            <h3 className="text-red-400 font-semibold">Critical Errors</h3>
            <p>
              Something failed while reading the table's metadata — for example,
              a file could not be accessed or the backend encountered an
              unexpected state. The graph may be incomplete or missing sections
              entirely.
            </p>
          </div>
          <div className="space-y-1">
            <h3 className="text-amber-400 font-semibold">Warnings</h3>
            <p>
              Your request exceeded the allowed data file limit. The backend
              stopped collecting data files at the configured maximum, so the
              graph represents a partial view of the table. The snapshot and
              manifest structure is still complete; only data file coverage is
              capped.
            </p>
          </div>
        </div>
        <p>
          The panel opens automatically when the backend reports any issue. Even
          so, always check it when the graph looks incomplete or the data file
          count seems lower than expected.
        </p>
      </div>
    ),
  },
  {
    id: "keyboard-shortcuts",
    title: "Keyboard Shortcuts",
    body: (
      <div className="space-y-6">
        <div className="space-y-1">
          <h3 className="text-white font-semibold mb-2">Global</h3>
          <ShortcutRow keys={["1"]} desc="Go to Timeline view" />
          <ShortcutRow keys={["2"]} desc="Go to Metadata view" />
          <ShortcutRow keys={["3"]} desc="Go to FileTree view" />
          <ShortcutRow keys={["4"]} desc="Go to Graph view" />
        </div>

        <p className="text-slate-400 text-xs">
          Throughout the app, <Key k="j" /> / <Key k="↓" /> and <Key k="k" /> /{" "}
          <Key k="↑" /> scroll the active panel or list.
        </p>

        <div className="space-y-1">
          <h3 className="text-white font-semibold mb-2">Docs Page</h3>
          <ShortcutRow
            keys={["k"]}
            desc="Open the documentation search overlay"
          />
          <ShortcutRow
            keys={["Ctrl", "n"]}
            desc="Select the next search result"
          />
          <ShortcutRow
            keys={["Ctrl", "p"]}
            desc="Select the previous search result"
          />
          <ShortcutRow
            keys={["Enter"]}
            desc="Open the selected search result"
          />
          <ShortcutRow keys={["Esc"]} desc="Close the search overlay" />
        </div>

        <div className="space-y-1">
          <h3 className="text-white font-semibold mb-2">Timeline View</h3>
          <ShortcutRow
            keys={["Shift", "Scroll"]}
            desc="Pan the timeline horizontally"
          />
          <ShortcutRow
            keys={["r"]}
            desc="Center and zoom to fit the entire timeline"
          />
          <ShortcutRow
            keys={["h", "←"]}
            desc="Select the previous snapshot — if none is selected, jumps to the first (oldest)"
          />
          <ShortcutRow
            keys={["l", "→"]}
            desc="Select the next snapshot — if none is selected, jumps to the last (newest)"
          />
          <ShortcutRow
            keys={["j", "↓"]}
            desc="Scroll the snapshot details panel down"
          />
          <ShortcutRow
            keys={["k", "↑"]}
            desc="Scroll the snapshot details panel up"
          />
          <ShortcutRow
            keys={["f"]}
            desc="Toggle fullscreen for the snapshot details panel (when open)"
          />
          <ShortcutRow keys={["Esc"]} desc="Close the snapshot details panel" />
        </div>

        <div className="space-y-1">
          <h3 className="text-white font-semibold mb-2">Metadata View</h3>
          <ShortcutRow keys={["j", "↓"]} desc="Scroll the page down" />
          <ShortcutRow keys={["k", "↑"]} desc="Scroll the page up" />
        </div>

        <div className="space-y-1">
          <h3 className="text-white font-semibold mb-2">Graph View</h3>
          <ShortcutRow
            keys={["c"]}
            desc="Center and zoom to fit the entire graph"
          />
          <ShortcutRow keys={["r"]} desc="Reset view to initial state" />
          <ShortcutRow
            keys={["i"]}
            desc="Toggle inspect mode (disables keyboard navigation so you can interact freely with the graph)"
          />
          <ShortcutRow
            keys={["Enter", "Space"]}
            desc="Jump to the main metadata node"
          />
          <ShortcutRow
            keys={["h", "←"]}
            desc="Navigate to the selected node's parent(s)"
          />
          <ShortcutRow
            keys={["l", "→"]}
            desc="Navigate to the selected node's child(ren)"
          />
          <ShortcutRow
            keys={["j", "↓"]}
            desc="Scroll the node details panel down (when open)"
          />
          <ShortcutRow
            keys={["k", "↑"]}
            desc="Scroll the node details panel up (when open)"
          />
          <ShortcutRow
            keys={["f"]}
            desc="Toggle fullscreen for the details panel (when open)"
          />
          <ShortcutRow keys={["Esc"]} desc="Close the node details panel" />
        </div>
      </div>
    ),
  },
  {
    id: "tips",
    title: "Tips & Tricks",
    body: (
      <div className="space-y-5">
        <div className="space-y-2">
          <h3 className="text-white font-semibold">
            Start with a narrow snapshot range
          </h3>
          <p>
            Loading all snapshots at once produces an overwhelming graph. Start
            with the 2–7 most recent snapshots and expand only if you need more
            history.
          </p>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">
            Duplicate tab for side-by-side comparison
          </h3>
          <p>
            Use the <strong className="text-white">Duplicate tab</strong> button
            in the navbar to open the current view in a new browser tab using
            cached data — no extra backend request. Load a different snapshot
            range in the original tab to compare two states of the same table.
          </p>
        </div>
      </div>
    ),
  },
  {
    id: "cli",
    title: "CLI & Python Client",
    body: (
      <div className="space-y-5">
        <p>
          <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
            icegraph-client
          </code>{" "}
          is a Python client and CLI for the same backend API this UI uses —
          handy for scripting access to tables, snapshots, and the metadata
          graph.
        </p>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Install</h3>
          <p>
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              icegraph-client
            </code>{" "}
            is only guaranteed compatible with the exact same version of the
            IceGraph server it talks to - they're released together. Install
            that version:
          </p>
          <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto">
            {PIP_INSTALL_COMMAND}
          </pre>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Point it at your server</h3>
          <p>
            Pass{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              --base-url
            </code>
            , or set it once via the{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              ICEGRAPH_BASE_URL
            </code>{" "}
            environment variable.
          </p>
          <p>
            If your server sits behind auth, pass{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              --token
            </code>{" "}
            (sent as an{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              Authorization: Bearer
            </code>{" "}
            header) or{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              --cookie
            </code>{" "}
            — or set them via the{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              ICEGRAPH_TOKEN
            </code>{" "}
            /{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              ICEGRAPH_COOKIE
            </code>{" "}
            environment variables.
          </p>
          <p>
            IceGraph itself doesn't have a login system, so you'll only need a
            token or cookie if your team has placed the server behind its own
            proxy. If you're already able to reach the IceGraph UI in a browser,
            that proxy has already authenticated your session - you can find the
            value it's using by opening DevTools → Application/Storage → Cookies
            (or the Network tab → any request → Request Headers) on the IceGraph
            page. If that doesn't apply, or you're not sure, it's best to check
            with whoever set up your IceGraph server - they'll know how their
            proxy handles authentication.
          </p>
          <p>
            If your server uses a self-signed or otherwise untrusted TLS
            certificate, pass{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              --no-verify-ssl
            </code>{" "}
            to skip certificate verification, or set the{" "}
            <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
              ICEGRAPH_NO_VERIFY_SSL
            </code>{" "}
            environment variable.
          </p>
        </div>
        <div className="space-y-2">
          <h3 className="text-white font-semibold">Commands</h3>
          <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap">{`icegraph tables
icegraph snapshots <table>
icegraph graph <table> [--start-snapshot-id ID] [--end-snapshot-id ID]`}</pre>
        </div>
        <p>
          Each command prints its result as JSON on stdout, so it pipes and
          redirects cleanly (e.g. into{" "}
          <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
            jq
          </code>
          , Python, or a file). Status messages go to stderr, so they never end
          up mixed into the JSON output.
        </p>
      </div>
    ),
  },
];

function extractText(node) {
  if (typeof node === "string") return node;

  if (Array.isArray(node)) {
    return node.map(extractText).join(" ");
  }

  const textProp = TEXT_PROP_BY_TYPE.get(node?.type);
  if (textProp) {
    return node.props[textProp];
  }

  if (node?.props?.children) {
    return extractText(node.props.children);
  }

  return "";
}

export default function DocsPage() {
  const [active, setActive] = useState(SECTIONS[0].id);
  const [searchOpen, setSearchOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlight, setHighlight] = useState(null);
  const [selectedResultIndex, setSelectedResultIndex] = useState(0);
  const contentRef = useRef(null);
  const activeMarkRef = useRef(null);
  const resultsContainerRef = useRef(null);
  const lastMousePos = useRef({ x: -1, y: -1 });

  const closeSearch = () => {
    setSearchOpen(false);
    setQuery("");
  };

  const selectResult = (result) => {
    setActive(result.section.id);
    setHighlight({
      sectionId: result.section.id,
      term: query,
      index: result.occurrenceIndex,
    });
    closeSearch();
  };

  const searchResults = query
    ? SECTIONS.flatMap((section) => {
        const content = extractText(section.body);
        const contentMatches = findAllIndices(content, query);

        return contentMatches.map((matchIndex, occurrenceIndex) => {
          const snippetStart = Math.max(0, matchIndex - 40);
          const snippet = content.substring(snippetStart, snippetStart + 140);

          return {
            section,
            snippet,
            occurrenceIndex,
            totalInSection: contentMatches.length,
          };
        });
      })
    : [];

  useEffect(() => {
    if (contentRef.current) contentRef.current.scrollTop = 0;
  }, [active]);

  useEffect(() => {
    if (highlight?.index != null && activeMarkRef.current) {
      activeMarkRef.current.scrollIntoView({
        behavior: "smooth",
        block: "center",
      });
    }
  }, [highlight, active]);

  useEffect(() => {
    resultsContainerRef.current?.children[selectedResultIndex]?.scrollIntoView({
      block: "nearest",
    });
  }, [selectedResultIndex]);

  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.key === "Escape") {
        closeSearch();
        return;
      }

      if (
        ["INPUT", "TEXTAREA", "SELECT"].includes(e.target.tagName) ||
        e.target.isContentEditable
      )
        return;

      if (e.key === "k") {
        e.preventDefault();
        setSearchOpen(true);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  const activeSection = SECTIONS.find((s) => s.id === active);

  return (
    <div className="flex flex-1 overflow-hidden">
      {searchOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-50 flex justify-center items-start pt-20 px-4"
          onClick={closeSearch}
        >
          <div
            className="w-full max-w-4xl h-[80vh] bg-surface-deep border border-edge rounded-lg shadow-xl flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <input
              autoFocus
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
                setSelectedResultIndex(0);
              }}
              onKeyDown={(e) => {
                if (e.ctrlKey && e.key === "n") {
                  e.preventDefault();
                  setSelectedResultIndex((i) =>
                    Math.min(i + 1, searchResults.length - 1),
                  );
                  return;
                }

                if (e.ctrlKey && e.key === "p") {
                  e.preventDefault();
                  setSelectedResultIndex((i) => Math.max(i - 1, 0));
                  return;
                }

                if (e.key === "Enter") {
                  const result = searchResults[selectedResultIndex];
                  if (!result) return;
                  e.preventDefault();
                  selectResult(result);
                }
              }}
              placeholder="Search documentation..."
              className="w-full px-4 py-3 bg-surface-hover text-white outline-none rounded-t-lg shrink-0"
            />

            {query && (
              <div className="border-t border-edge flex-1 min-h-0">
                {searchResults.length > 0 ? (
                  <div
                    ref={resultsContainerRef}
                    className="h-full overflow-y-auto"
                  >
                    {searchResults.map((result, index) => (
                      <button
                        key={`${result.section.id}-${result.occurrenceIndex}`}
                        onMouseMove={(e) => {
                          if (
                            e.clientX === lastMousePos.current.x &&
                            e.clientY === lastMousePos.current.y
                          )
                            return;
                          lastMousePos.current = { x: e.clientX, y: e.clientY };
                          setSelectedResultIndex(index);
                        }}
                        onClick={() => selectResult(result)}
                        className={`w-full text-left p-4 border-b border-edge ${
                          index === selectedResultIndex
                            ? "bg-surface-hover"
                            : ""
                        }`}
                      >
                        <div className="text-white font-semibold text-lg">
                          {result.section.title}
                        </div>

                        <div className="text-accent text-xs mt-1">
                          Found in: {result.section.title}
                          {result.totalInSection > 1 &&
                            ` — match ${result.occurrenceIndex + 1} of ${result.totalInSection}`}
                        </div>

                        <div className="text-slate-400 text-sm mt-2 leading-relaxed">
                          {highlightMatch(result.snippet, query)}
                        </div>
                      </button>
                    ))}
                  </div>
                ) : (
                  <div className="p-4 text-slate-400">No results found.</div>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      <aside className="w-52 shrink-0 bg-[#151b26] border-r border-edge overflow-y-auto hidden sm:block">
        <div className="px-4 py-5">
          <div className="mb-4">
            <p className={UI_DOCS_NAV_TITLE_CLASS}>Documentation</p>

            <button
              onClick={() => setSearchOpen(true)}
              className="w-full flex items-center justify-between rounded-md border border-edge bg-surface px-3 py-2 text-sm text-slate-400 hover:text-white hover:bg-surface-hover transition"
            >
              <span>Search docs...</span>
              <Key k="k" />
            </button>
          </div>

          <nav className="flex flex-col gap-0.5">
            {SECTIONS.map((s) => (
              <button
                key={s.id}
                onClick={() => {
                  setActive(s.id);
                  setHighlight(null);
                }}
                className={`text-left text-sm px-3 py-2 rounded-md transition ${
                  active === s.id
                    ? "bg-accent-muted text-white font-medium"
                    : "text-slate-400 hover:text-white hover:bg-surface-hover"
                }`}
              >
                {s.title}
              </button>
            ))}
          </nav>
        </div>
      </aside>

      <div className="flex-1 overflow-y-auto" ref={contentRef}>
        <div className="sm:hidden px-4 pt-4 pb-2">
          <select
            value={active}
            onChange={(e) => {
              setActive(e.target.value);
              setHighlight(null);
            }}
            className="w-full bg-surface-hover text-white text-sm border border-edge rounded-md px-3 py-2"
          >
            {SECTIONS.map((s) => (
              <option key={s.id} value={s.id}>
                {s.title}
              </option>
            ))}
          </select>
        </div>

        <div className="max-w-3xl mx-auto px-6 py-8">
          <h1 className="text-2xl font-bold text-white mb-6">
            {activeSection.title}
          </h1>
          <div className={UI_DOCS_BODY_CLASS}>
            {highlight?.sectionId === activeSection.id
              ? highlightTreeMatches(
                  activeSection.body,
                  highlight.term,
                  { count: 0, target: highlight.index, key: 0 },
                  activeMarkRef,
                )
              : activeSection.body}
          </div>
        </div>
      </div>
    </div>
  );
}
