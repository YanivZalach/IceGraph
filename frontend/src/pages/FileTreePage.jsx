import { useEffect, useMemo, useRef, useState } from "react";
import { useTableGraphData } from "../features/table/tableGraphData";
import PanelIssueNotice from "../components/PanelIssueNotice";
import { FileType, fileTypeLabel } from "../graphConstants";
import { useViewInGraph } from "../hooks/useViewInGraph";
import {
  UI_BODY_MUTED_ITALIC_CLASS,
  UI_FIELD_LABEL_MB_CLASS,
  UI_FILE_COUNT_BADGE_CLASS,
  UI_HELPER_TEXT_CLASS,
  UI_MONO_MUTED_CLASS,
  UI_MONO_MUTED_NOWRAP_CLASS,
  UI_MONO_VALUE_CLASS,
} from "../uiTypography";

const FILE_TYPES = new Set([
  FileType.DATA,
  FileType.POSITION_DELETE,
  FileType.EQUALITY_DELETE,
]);

function Dropdown({ triggerLabel, isOpen, onToggle, dropdownRef, children }) {
  return (
    <div ref={dropdownRef} className="relative">
      <button
        onClick={onToggle}
        className={`flex items-center gap-2 text-sm px-3 py-1.5 rounded-lg border transition cursor-pointer select-none ${
          isOpen
            ? "bg-accent-muted/40 border-accent text-white"
            : "bg-surface border-edge text-ink hover:border-edge-hover"
        }`}
      >
        <span className="font-medium">{triggerLabel}</span>
        <svg
          className={`w-3.5 h-3.5 text-slate-400 transition-transform ${isOpen ? "rotate-180" : ""}`}
          viewBox="0 0 16 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
        >
          <path d="M4 6l4 4 4-4" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </button>
      {isOpen && (
        <div className="absolute top-full left-0 mt-1 z-50 bg-surface border border-edge rounded-xl shadow-2xl overflow-hidden min-w-[160px] max-h-60 overflow-y-auto">
          {children}
        </div>
      )}
    </div>
  );
}

function DropdownItem({ label, badge, active, onClick }) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center justify-between px-4 py-2 text-sm transition cursor-pointer ${
        active
          ? "bg-accent-muted text-white"
          : "text-slate-300 hover:bg-surface-hover hover:text-white"
      }`}
    >
      <span>{label}</span>
      {badge && (
        <span className="text-xs font-bold uppercase tracking-wider text-accent ml-3">
          {badge}
        </span>
      )}
    </button>
  );
}

function getAllFilesFromNode(node) {
  const result = [...node.files];
  for (const child of Object.values(node.children)) {
    result.push(...getAllFilesFromNode(child));
  }
  return result;
}

function getAllTreePaths(node, prefix) {
  const paths = [];
  for (const [label, child] of Object.entries(node.children)) {
    const path = prefix ? `${prefix}/${label}` : label;
    paths.push(path);
    paths.push(...getAllTreePaths(child, path));
  }
  return paths;
}

function getFolderLastModified(node, fileTimestampMap) {
  const files = getAllFilesFromNode(node);
  const timestamps = files.map((f) => fileTimestampMap[f]).filter(Boolean);
  return timestamps.length > 0
    ? timestamps.reduce((a, b) => (a > b ? a : b))
    : null;
}

function buildTree(partitions) {
  const root = { children: {}, files: [] };
  for (const [partitionStr, files] of partitions) {
    if (partitionStr === "(unpartitioned)") {
      root.files.push(...files);
      continue;
    }
    const segments = partitionStr.split(", ");
    let node = root;
    for (const segment of segments) {
      if (!node.children[segment]) {
        node.children[segment] = { children: {}, files: [] };
      }
      node = node.children[segment];
    }
    node.files.push(...files);
  }
  return root;
}

function getSnapshotFileErrors(snapshot, adjacency, nodeById) {
  if (!snapshot) return [];

  const errors = [];
  const visited = new Set();
  const queue = [snapshot.id];

  while (queue.length > 0) {
    const current = queue.shift();
    if (visited.has(current)) continue;
    visited.add(current);

    const node = nodeById[current];
    if (!node) continue;

    if (node.details?.error) {
      errors.push(
        `${fileTypeLabel(node.type)} (${node.label || node.id}): ${node.details.error}`,
      );
    }

    for (const { to } of adjacency[current] || []) queue.push(to);
  }

  return errors;
}

function FileRow({
  filePath,
  checkedFiles,
  toggleFile,
  viewInGraph,
  duplicatingNodeId,
  timestamp,
}) {
  return (
    <div
      onClick={() => toggleFile(filePath)}
      className={`flex items-center gap-2.5 px-3 py-2 rounded-md border transition cursor-pointer group ${
        checkedFiles.has(filePath)
          ? "bg-accent-muted border-accent/40"
          : "bg-canvas border-transparent hover:bg-[#131c2b] hover:border-edge"
      }`}
    >
      <input
        type="checkbox"
        checked={checkedFiles.has(filePath)}
        onChange={() => toggleFile(filePath)}
        onClick={(e) => e.stopPropagation()}
        className="w-3.5 h-3.5 rounded accent-[#2E86C1] cursor-pointer shrink-0"
      />
      <span
        className={`text-xs font-mono transition-colors overflow-hidden whitespace-nowrap flex-1 ${checkedFiles.has(filePath) ? "text-slate-200" : "text-slate-400 group-hover:text-slate-200"}`}
        style={{
          direction: "rtl",
          textOverflow: "ellipsis",
          textAlign: "left",
        }}
        title={filePath}
      >
        {"\u202A" + filePath + "\u202C"}
      </span>
      {timestamp && (
        <span
          className={`${UI_MONO_MUTED_NOWRAP_CLASS} shrink-0`}
          title="First appearing snapshot timestamp"
        >
          {timestamp}
        </span>
      )}
      <button
        onClick={(e) => viewInGraph(e, filePath)}
        disabled={!!duplicatingNodeId}
        title={duplicatingNodeId === filePath ? "Opening…" : "View in graph"}
        className="shrink-0 ml-2 p-1 rounded text-slate-500 hover:text-accent hover:bg-accent-muted transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
      >
        <svg
          className="w-3.5 h-3.5"
          viewBox="0 0 16 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
        >
          <circle cx="4" cy="8" r="2" />
          <circle cx="12" cy="4" r="2" />
          <circle cx="12" cy="12" r="2" />
          <path d="M6 7.2L10 4.8M6 8.8L10 11.2" strokeLinecap="round" />
        </svg>
      </button>
    </div>
  );
}

function TreeNode({
  label,
  node,
  path,
  checkedFiles,
  toggleFile,
  toggleBulk,
  viewInGraph,
  duplicatingNodeId,
  collapsed,
  toggleCollapse,
  setCollapsed,
  fileTimestampMap,
}) {
  const allFiles = getAllFilesFromNode(node);
  const allChecked =
    allFiles.length > 0 && allFiles.every((f) => checkedFiles.has(f));
  const someChecked = !allChecked && allFiles.some((f) => checkedFiles.has(f));
  const folderLastModified = getFolderLastModified(node, fileTimestampMap);
  const isCollapsed = collapsed[path];
  const sortedChildren = Object.entries(node.children).sort(([a], [b]) =>
    b.localeCompare(a),
  );
  const hasChildFolders = sortedChildren.length > 0;

  const expandInner = (e) => {
    e.stopPropagation();
    const subPaths = getAllTreePaths(node, path);
    setCollapsed((prev) => {
      const next = { ...prev };
      delete next[path]; // open this folder too if it was closed
      for (const p of subPaths) delete next[p];
      return next;
    });
  };

  const collapseInner = (e) => {
    e.stopPropagation();
    const subPaths = getAllTreePaths(node, path);
    setCollapsed((prev) => ({
      ...prev,
      [path]: true,
      ...Object.fromEntries(subPaths.map((p) => [p, true])),
    }));
  };

  return (
    <div className="bg-surface rounded-lg border border-edge overflow-hidden">
      <div
        className="flex items-center px-4 py-2.5 hover:bg-surface-hover transition cursor-pointer"
        onClick={() => toggleCollapse(path)}
      >
        <div className="flex items-center gap-3 min-w-0 flex-1">
          <svg
            className={`w-3.5 h-3.5 text-accent shrink-0 transition-transform ${isCollapsed ? "-rotate-90" : ""}`}
            viewBox="0 0 16 16"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.5"
          >
            <path
              d="M4 6l4 4 4-4"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          <svg
            className="w-4 h-4 text-slate-400 shrink-0"
            viewBox="0 0 20 20"
            fill="currentColor"
          >
            <path d="M2 6a2 2 0 012-2h4l2 2h6a2 2 0 012 2v6a2 2 0 01-2 2H4a2 2 0 01-2-2V6z" />
          </svg>
          <span className={UI_MONO_VALUE_CLASS}>{label}</span>
        </div>
        <div className="flex items-center gap-2 ml-4 shrink-0">
          {folderLastModified && (
            <span
              className={UI_MONO_MUTED_NOWRAP_CLASS}
              title="Last modified (earliest appearing snapshot)"
            >
              {folderLastModified}
            </span>
          )}
          <span className={UI_FILE_COUNT_BADGE_CLASS}>{allFiles.length}</span>
          {hasChildFolders && (
            <>
              <button
                onClick={expandInner}
                title="Expand inner folders"
                className="p-1 rounded text-slate-600 hover:text-slate-300 hover:bg-edge transition-colors cursor-pointer"
              >
                <svg
                  className="w-3.5 h-3.5"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.8"
                >
                  <path
                    d="M4 5l4 4 4-4"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                  <path
                    d="M4 9l4 4 4-4"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </button>
              <button
                onClick={collapseInner}
                title="Collapse inner folders"
                className="p-1 rounded text-slate-600 hover:text-slate-300 hover:bg-edge transition-colors cursor-pointer"
              >
                <svg
                  className="w-3.5 h-3.5"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.8"
                >
                  <path
                    d="M4 11l4-4 4 4"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                  <path
                    d="M4 7l4-4 4 4"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </button>
            </>
          )}
          <input
            type="checkbox"
            checked={allChecked}
            ref={(el) => {
              if (el) el.indeterminate = someChecked;
            }}
            onChange={() => toggleBulk(allFiles)}
            onClick={(e) => e.stopPropagation()}
            className="w-3.5 h-3.5 rounded accent-[#2E86C1] cursor-pointer"
            title="Select all in folder"
          />
        </div>
      </div>

      {!isCollapsed && (
        <div className="border-t border-edge px-4 py-2 flex flex-col gap-2">
          {sortedChildren.map(([childLabel, childNode]) => (
            <TreeNode
              key={childLabel}
              label={childLabel}
              node={childNode}
              path={`${path}/${childLabel}`}
              checkedFiles={checkedFiles}
              toggleFile={toggleFile}
              toggleBulk={toggleBulk}
              viewInGraph={viewInGraph}
              duplicatingNodeId={duplicatingNodeId}
              collapsed={collapsed}
              toggleCollapse={toggleCollapse}
              setCollapsed={setCollapsed}
              fileTimestampMap={fileTimestampMap}
            />
          ))}
          {node.files.length > 0 && (
            <div className="flex flex-col gap-1">
              {node.files.map((filePath) => (
                <FileRow
                  key={filePath}
                  filePath={filePath}
                  checkedFiles={checkedFiles}
                  toggleFile={toggleFile}
                  viewInGraph={viewInGraph}
                  duplicatingNodeId={duplicatingNodeId}
                  timestamp={fileTimestampMap[filePath]}
                />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default function FileTreePage() {
  const { nodes, edges, metadata } = useTableGraphData();
  const { viewInGraph, duplicatingNodeId } = useViewInGraph();
  const [search, setSearch] = useState("");
  const [selectedBranch, setSelectedBranch] = useState(null);
  const [selectedIdx, setSelectedIdx] = useState(null);
  const [collapsed, setCollapsed] = useState({});
  const [checkedFiles, setCheckedFiles] = useState(new Set());
  const [copied, setCopied] = useState(false);
  const [copiedSnapshotId, setCopiedSnapshotId] = useState(false);
  const [branchDropdownOpen, setBranchDropdownOpen] = useState(false);
  const [snapshotDropdownOpen, setSnapshotDropdownOpen] = useState(false);
  const [viewMode, setViewMode] = useState("tree"); // 'flat' | 'tree'
  const branchDropdownRef = useRef(null);
  const snapshotDropdownRef = useRef(null);

  useEffect(() => {
    if (!branchDropdownOpen) return;
    const handler = (e) => {
      if (
        branchDropdownRef.current &&
        !branchDropdownRef.current.contains(e.target)
      )
        setBranchDropdownOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [branchDropdownOpen]);

  useEffect(() => {
    if (!snapshotDropdownOpen) return;
    const handler = (e) => {
      if (
        snapshotDropdownRef.current &&
        !snapshotDropdownRef.current.contains(e.target)
      )
        setSnapshotDropdownOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [snapshotDropdownOpen]);

  const { snapshots, adjacency, nodeById, snapshotById } = useMemo(() => {
    const allNodes = nodes || [];
    const allEdges = edges || [];

    const byId = {};
    for (const n of allNodes) byId[n.id] = n;

    const snaps = allNodes
      .filter((n) => n.type === FileType.SNAPSHOT)
      .sort((a, b) => (a.details.timestamp || 0) - (b.details.timestamp || 0));

    const snapById = {};
    for (const s of snaps) {
      if (s.details.snapshot_id) snapById[s.details.snapshot_id] = s;
    }

    const adj = {};
    for (const e of allEdges) {
      if (!adj[e.from]) adj[e.from] = [];
      adj[e.from].push({ to: e.to, is_deleted: !!e.is_deleted });
    }

    return {
      snapshots: snaps,
      adjacency: adj,
      nodeById: byId,
      snapshotById: snapById,
    };
  }, [nodes, edges]);

  const branches = useMemo(() => {
    if (!metadata?.refs) return [];
    return Object.entries(metadata.refs)
      .filter(([, ref]) => ref.type === "branch")
      .map(([name, ref]) => ({
        name,
        headSnapshotId: String(ref["snapshot-id"]),
      }))
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [metadata]);

  useEffect(() => {
    if (selectedBranch !== null) return;
    const mainBranch = branches.find((b) => b.name === "main");
    if (mainBranch) setSelectedBranch("main");
  }, [branches]);

  const displayedSnapshots = useMemo(() => {
    if (!selectedBranch) return snapshots;
    const branch = branches.find((b) => b.name === selectedBranch);
    if (!branch) return snapshots;

    const result = [];
    const visited = new Set();
    let currentId = branch.headSnapshotId;
    while (currentId && !visited.has(currentId)) {
      visited.add(currentId);
      const node = snapshotById[currentId];
      if (!node) break;
      result.push(node);
      currentId = node.details.parent_id;
    }
    return result.reverse();
  }, [selectedBranch, branches, snapshots, snapshotById]);

  const effectiveIdx =
    selectedIdx !== null ? selectedIdx : displayedSnapshots.length - 1;

  const partitionMap = useMemo(() => {
    if (displayedSnapshots.length === 0) return {};
    const snapshot = displayedSnapshots[effectiveIdx];
    if (!snapshot) return {};

    const visited = new Set();
    const queue = [snapshot.id];
    const dataFiles = [];

    while (queue.length > 0) {
      const current = queue.shift();
      if (visited.has(current)) continue;
      visited.add(current);

      for (const { to, is_deleted } of adjacency[current] || []) {
        const child = nodeById[to];
        if (!child) continue;
        if (FILE_TYPES.has(child.type)) {
          if (!is_deleted) dataFiles.push(child);
        } else if (child.type === FileType.MANIFEST) {
          queue.push(to);
        }
      }
    }

    const partMap = {};
    for (const f of dataFiles) {
      const partition = f.details.partition || "(unpartitioned)";
      if (!partMap[partition]) partMap[partition] = [];
      partMap[partition].push(f.id);
    }
    return partMap;
  }, [displayedSnapshots, effectiveIdx, adjacency, nodeById]);

  const filteredPartitions = useMemo(() => {
    const q = search.trim().toLowerCase();
    return Object.entries(partitionMap)
      .filter(([part]) => !q || part.toLowerCase().includes(q))
      .sort(([a], [b]) => b.localeCompare(a));
  }, [partitionMap, search]);

  const treeData = useMemo(
    () => buildTree(filteredPartitions),
    [filteredPartitions],
  );

  const fileTimestampMap = useMemo(() => {
    const map = {};
    for (const files of Object.values(partitionMap)) {
      for (const filePath of files) {
        const ts =
          nodeById[filePath]?.details?.earliest_appearing_snapshot_timestamp;
        if (ts) map[filePath] = ts;
      }
    }
    return map;
  }, [partitionMap, nodeById]);

  const totalPartitions = filteredPartitions.length;
  const totalFiles = filteredPartitions.reduce(
    (sum, [, f]) => sum + f.length,
    0,
  );

  useEffect(() => {
    if (viewMode === "flat") {
      setCollapsed(
        Object.fromEntries(Object.keys(partitionMap).map((p) => [p, true])),
      );
    } else {
      const fullTree = buildTree(Object.entries(partitionMap));
      setCollapsed(
        Object.fromEntries(getAllTreePaths(fullTree, "").map((p) => [p, true])),
      );
    }
  }, [partitionMap, viewMode]);

  const resetSelection = () => {
    setSelectedIdx(null);
    setCheckedFiles(new Set());
  };

  const toggleCollapse = (key) =>
    setCollapsed((prev) => ({ ...prev, [key]: !prev[key] }));

  const toggleFile = (path) =>
    setCheckedFiles((prev) => {
      const next = new Set(prev);
      next.has(path) ? next.delete(path) : next.add(path);
      return next;
    });

  const toggleBulk = (files) => {
    const allChecked = files.every((f) => checkedFiles.has(f));
    setCheckedFiles((prev) => {
      const next = new Set(prev);
      files.forEach((f) => (allChecked ? next.delete(f) : next.add(f)));
      return next;
    });
  };

  const collapseAll = () => {
    if (viewMode === "flat") {
      setCollapsed(
        Object.fromEntries(filteredPartitions.map(([p]) => [p, true])),
      );
    } else {
      setCollapsed(
        Object.fromEntries(getAllTreePaths(treeData, "").map((p) => [p, true])),
      );
    }
  };

  const expandAll = () => setCollapsed({});

  const copyPaths = () => {
    navigator.clipboard.writeText([...checkedFiles].join("\n"));
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  if (snapshots.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center bg-canvas">
        <p className={UI_BODY_MUTED_ITALIC_CLASS}>No snapshots available.</p>
      </div>
    );
  }

  const currentSnapshot = displayedSnapshots[effectiveIdx];
  const snapshotFileErrors = getSnapshotFileErrors(
    currentSnapshot,
    adjacency,
    nodeById,
  );

  return (
    <div className="flex-1 flex flex-col bg-canvas overflow-hidden">
      <div className="shrink-0 px-4 sm:px-6 py-3 flex items-center flex-wrap gap-x-3 gap-y-2 border-b border-edge">
        <div className="flex items-center gap-2">
          {branches.length > 0 && (
            <>
              <Dropdown
                dropdownRef={branchDropdownRef}
                isOpen={branchDropdownOpen}
                onToggle={() => setBranchDropdownOpen((p) => !p)}
                triggerLabel={
                  selectedBranch ? (
                    <>{selectedBranch}</>
                  ) : (
                    <span className="text-slate-400">All branches</span>
                  )
                }
              >
                <DropdownItem
                  label="All branches"
                  active={selectedBranch === null}
                  onClick={() => {
                    setSelectedBranch(null);
                    resetSelection();
                    setBranchDropdownOpen(false);
                  }}
                />
                <div className="h-px bg-edge mx-2" />
                {branches.map((b) => (
                  <DropdownItem
                    key={b.name}
                    label={b.name}
                    active={selectedBranch === b.name}
                    onClick={() => {
                      setSelectedBranch(b.name);
                      resetSelection();
                      setBranchDropdownOpen(false);
                    }}
                  />
                ))}
              </Dropdown>
              <div className="w-px h-4 bg-slate-700" />
            </>
          )}

          <Dropdown
            dropdownRef={snapshotDropdownRef}
            isOpen={snapshotDropdownOpen}
            onToggle={() => setSnapshotDropdownOpen((p) => !p)}
            triggerLabel={
              <span className="flex flex-col items-start leading-none gap-1">
                <span className="flex items-center gap-1.5 text-base text-slate-400">
                  Snapshot {effectiveIdx + 1}
                  {effectiveIdx === displayedSnapshots.length - 1 && (
                    <span className="text-xs font-bold uppercase tracking-wider text-accent">
                      latest
                    </span>
                  )}
                </span>
                {currentSnapshot?.details.snapshot_id && (
                  <span className={UI_MONO_MUTED_CLASS}>
                    {currentSnapshot.details.snapshot_id}
                  </span>
                )}
              </span>
            }
          >
            {displayedSnapshots.map((snap, i) => (
              <DropdownItem
                key={snap.id}
                label={`Snapshot ${i + 1}`}
                badge={i === displayedSnapshots.length - 1 ? "latest" : null}
                active={i === effectiveIdx}
                onClick={() => {
                  setSelectedIdx(i);
                  setCollapsed({});
                  setCheckedFiles(new Set());
                  setSnapshotDropdownOpen(false);
                }}
              />
            ))}
          </Dropdown>

          {currentSnapshot?.details.snapshot_id && (
            <button
              onClick={() => {
                navigator.clipboard.writeText(
                  currentSnapshot.details.snapshot_id,
                );
                setCopiedSnapshotId(true);
                setTimeout(() => setCopiedSnapshotId(false), 2000);
              }}
              title="Copy snapshot ID"
              className="p-1 rounded border border-edge text-slate-500 hover:text-slate-300 hover:border-slate-500 transition-colors cursor-pointer"
            >
              {copiedSnapshotId ? (
                <svg
                  className="w-3.5 h-3.5 text-green-400"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2.5"
                >
                  <path
                    d="M3 8l3.5 3.5L13 4"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              ) : (
                <svg
                  className="w-3.5 h-3.5"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.8"
                >
                  <rect x="5" y="5" width="8" height="9" rx="1.5" />
                  <path
                    d="M11 5V4a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h1"
                    strokeLinecap="round"
                  />
                </svg>
              )}
            </button>
          )}

          <div className="group relative">
            <div className="w-4 h-4 rounded-full bg-accent text-white text-tiny font-black flex items-center justify-center cursor-help hover:bg-accent-dark transition select-none">
              i
            </div>
            <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-56 bg-surface text-slate-300 text-detail p-3 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 leading-relaxed border border-edge">
              Snapshots are numbered in chronological order — Snapshot 1 is the
              oldest, the highest number is the latest.
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 border-8 border-transparent border-b-surface" />
            </div>
          </div>
        </div>

        <input
          type="text"
          placeholder="Search partitions…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="flex-1 min-w-30 max-w-xs text-sm bg-surface border border-edge text-ink rounded-lg px-3 py-1.5 placeholder-slate-500 focus:outline-none focus:border-accent"
        />

        <div className="ml-auto flex items-center gap-2">
          <div className="flex items-center rounded-lg border border-edge overflow-hidden">
            <button
              className={`flex items-center gap-1.5 text-sm px-3 py-1.5 transition cursor-pointer ${viewMode === "flat" ? "bg-accent text-white" : "text-slate-400 hover:text-slate-200 hover:bg-surface-hover"}`}
              onClick={() => setViewMode("flat")}
              title="Flat list view"
            >
              <svg
                className="w-3.5 h-3.5"
                viewBox="0 0 16 16"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.8"
              >
                <path d="M2 4h12M2 8h12M2 12h12" strokeLinecap="round" />
              </svg>
              Flat
            </button>
            <div className="w-px h-full bg-edge" />
            <button
              className={`flex items-center gap-1.5 text-sm px-3 py-1.5 transition cursor-pointer ${viewMode === "tree" ? "bg-accent text-white" : "text-slate-400 hover:text-slate-200 hover:bg-surface-hover"}`}
              onClick={() => setViewMode("tree")}
              title="Nested tree view"
            >
              <svg
                className="w-3.5 h-3.5"
                viewBox="0 0 16 16"
                fill="none"
                stroke="currentColor"
                strokeWidth="1.8"
              >
                <path d="M2 3h4M2 8h4M2 13h4" strokeLinecap="round" />
                <path d="M8 3h6M8 8h6M8 13h6" strokeLinecap="round" />
                <path d="M4 3v10" strokeLinecap="round" strokeDasharray="1 2" />
              </svg>
              Tree
            </button>
          </div>

          <div className="w-px h-4 bg-slate-700" />

          <button
            onClick={expandAll}
            disabled={totalPartitions === 0}
            title="Expand all"
            className="p-1.5 rounded border border-edge text-slate-400 hover:text-slate-200 hover:border-slate-500 transition disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer"
          >
            <svg
              className="w-4 h-4"
              viewBox="0 0 16 16"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.8"
            >
              <path
                d="M4 5l4 4 4-4"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <path
                d="M4 9l4 4 4-4"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </button>
          <button
            onClick={collapseAll}
            disabled={totalPartitions === 0}
            title="Collapse all"
            className="p-1.5 rounded border border-edge text-slate-400 hover:text-slate-200 hover:border-slate-500 transition disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer"
          >
            <svg
              className="w-4 h-4"
              viewBox="0 0 16 16"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.8"
            >
              <path
                d="M4 11l4-4 4 4"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <path
                d="M4 7l4-4 4 4"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </button>

          <div className="w-px h-4 bg-slate-700" />

          <button
            onClick={() =>
              setCheckedFiles(new Set(Object.values(partitionMap).flat()))
            }
            disabled={totalFiles === 0}
            className="text-sm px-3 py-1.5 rounded-lg border border-edge text-slate-400 hover:border-slate-500 hover:text-slate-200 transition disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer"
          >
            Select all
          </button>
          <button
            onClick={() => setCheckedFiles(new Set())}
            disabled={checkedFiles.size === 0}
            className="text-sm px-3 py-1.5 rounded-lg border border-edge text-slate-400 hover:border-slate-500 hover:text-slate-200 transition disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer"
          >
            Clear
          </button>

          <div className="w-px h-4 bg-slate-700" />

          <button
            onClick={copyPaths}
            disabled={checkedFiles.size === 0}
            className={`flex items-center gap-2 text-sm px-3 py-1.5 rounded-lg border transition ${
              checkedFiles.size === 0
                ? "border-edge text-slate-600 cursor-not-allowed"
                : copied
                  ? "border-green-600 bg-green-900/30 text-green-400"
                  : "border-accent text-accent hover:bg-accent-muted cursor-pointer"
            }`}
          >
            {copied ? (
              <>
                <svg
                  className="w-3.5 h-3.5"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <path
                    d="M3 8l3.5 3.5L13 4"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
                Copied!
              </>
            ) : (
              <>
                <svg
                  className="w-3.5 h-3.5"
                  viewBox="0 0 16 16"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="1.8"
                >
                  <rect x="5" y="5" width="8" height="9" rx="1.5" />
                  <path
                    d="M11 5V4a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h1"
                    strokeLinecap="round"
                  />
                </svg>
                Copy paths{" "}
                {checkedFiles.size > 0 && (
                  <span className="text-xs font-bold">
                    ({checkedFiles.size})
                  </span>
                )}
              </>
            )}
          </button>

          <div className="group relative pl-1">
            <span
              className={`${UI_HELPER_TEXT_CLASS} select-none whitespace-nowrap cursor-default`}
            >
              {totalPartitions} / {totalFiles}
            </span>
            <div className="absolute top-full right-0 mt-2 hidden group-hover:flex flex-col gap-1 bg-surface border border-edge rounded-lg shadow-xl px-3 py-2 text-xs text-slate-300 whitespace-nowrap z-50">
              <div className="absolute bottom-full right-3 border-8 border-transparent border-b-[#2d3748]" />
              <div className="flex items-center justify-between gap-4">
                <span className="text-slate-500">Partitions</span>
                <span className="font-semibold text-white">
                  {totalPartitions}
                </span>
              </div>
              <div className="flex items-center justify-between gap-4">
                <span className="text-slate-500">Files</span>
                <span className="font-semibold text-white">{totalFiles}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-4 sm:px-8 py-4 flex flex-col gap-2">
        {snapshotFileErrors.length > 0 && (
          <PanelIssueNotice type="error">
            {snapshotFileErrors.join("\n")}
          </PanelIssueNotice>
        )}

        {totalPartitions === 0 && (
          <p className={`${UI_BODY_MUTED_ITALIC_CLASS} mt-4`}>
            {search
              ? "No partitions match the search."
              : "No data files found for this snapshot."}
          </p>
        )}

        {viewMode === "flat" &&
          filteredPartitions.map(([partition, files]) => {
            const allChecked = files.every((f) => checkedFiles.has(f));
            const someChecked =
              !allChecked && files.some((f) => checkedFiles.has(f));
            const flatFolderLastModified = files
              .map((f) => fileTimestampMap[f])
              .filter(Boolean)
              .reduce((a, b) => (a > b ? a : b), null);
            return (
              <div
                key={partition}
                className="bg-surface rounded-lg border border-edge overflow-hidden"
              >
                <div
                  className="flex items-center px-4 py-2.5 hover:bg-surface-hover transition cursor-pointer"
                  onClick={() => toggleCollapse(partition)}
                >
                  <div className="flex items-center gap-3 min-w-0 flex-1">
                    <svg
                      className={`w-3.5 h-3.5 text-accent shrink-0 transition-transform ${collapsed[partition] ? "-rotate-90" : ""}`}
                      viewBox="0 0 16 16"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2.5"
                    >
                      <path
                        d="M4 6l4 4 4-4"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      />
                    </svg>
                    <span className={UI_MONO_VALUE_CLASS}>{partition}</span>
                  </div>
                  <div className="flex items-center gap-3 ml-4 shrink-0">
                    {flatFolderLastModified && (
                      <span
                        className={UI_MONO_MUTED_NOWRAP_CLASS}
                        title="Last modified (earliest appearing snapshot)"
                      >
                        {flatFolderLastModified}
                      </span>
                    )}
                    <span className={UI_FILE_COUNT_BADGE_CLASS}>
                      {files.length}
                    </span>
                    <input
                      type="checkbox"
                      checked={allChecked}
                      ref={(el) => {
                        if (el) el.indeterminate = someChecked;
                      }}
                      onChange={() => toggleBulk(files)}
                      onClick={(e) => e.stopPropagation()}
                      className="w-3.5 h-3.5 rounded accent-[#2E86C1] cursor-pointer"
                      title="Select all in partition"
                    />
                  </div>
                </div>
                {!collapsed[partition] && (
                  <div className="border-t border-edge px-4 py-2 flex flex-col gap-1">
                    {files.map((filePath) => (
                      <FileRow
                        key={filePath}
                        filePath={filePath}
                        checkedFiles={checkedFiles}
                        toggleFile={toggleFile}
                        viewInGraph={viewInGraph}
                        duplicatingNodeId={duplicatingNodeId}
                        timestamp={fileTimestampMap[filePath]}
                      />
                    ))}
                  </div>
                )}
              </div>
            );
          })}

        {viewMode === "tree" && totalPartitions > 0 && (
          <>
            {treeData.files.length > 0 && (
              <div className="bg-surface rounded-lg border border-edge px-4 py-2 flex flex-col gap-1">
                <span className={UI_FIELD_LABEL_MB_CLASS}>(unpartitioned)</span>
                {treeData.files.map((filePath) => (
                  <FileRow
                    key={filePath}
                    filePath={filePath}
                    checkedFiles={checkedFiles}
                    toggleFile={toggleFile}
                    viewInGraph={viewInGraph}
                    duplicatingNodeId={duplicatingNodeId}
                    timestamp={fileTimestampMap[filePath]}
                  />
                ))}
              </div>
            )}
            {Object.entries(treeData.children)
              .sort(([a], [b]) => b.localeCompare(a))
              .map(([label, node]) => (
                <TreeNode
                  key={label}
                  label={label}
                  node={node}
                  path={label}
                  checkedFiles={checkedFiles}
                  toggleFile={toggleFile}
                  toggleBulk={toggleBulk}
                  viewInGraph={viewInGraph}
                  duplicatingNodeId={duplicatingNodeId}
                  collapsed={collapsed}
                  toggleCollapse={toggleCollapse}
                  setCollapsed={setCollapsed}
                  fileTimestampMap={fileTimestampMap}
                />
              ))}
          </>
        )}
      </div>
    </div>
  );
}
