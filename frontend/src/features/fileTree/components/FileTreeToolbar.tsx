import { useState } from "react";
import type {
  Branch,
  FileTreeViewMode,
  SnapshotFileScope,
  SnapshotNode,
} from "../types";
import FileTreeViewSettings from "./FileTreeViewSettings";

interface FileTreeToolbarProps {
  branches: Branch[];
  checkedFileIds: Set<string>;
  currentSnapshotId: string;
  fileCount: number;
  onBranchChange: (branchName: string | null) => void;
  onClearSelection: () => void;
  onCollapseAll: () => void;
  onExpandAll: () => void;
  onScopeChange: (scope: SnapshotFileScope) => void;
  onSearchChange: (search: string) => void;
  onSelectAll: () => void;
  onSnapshotChange: (snapshotId: string) => void;
  onViewModeChange: (viewMode: FileTreeViewMode) => void;
  partitionCount: number;
  scope: SnapshotFileScope;
  search: string;
  selectedBranchName: string | null;
  snapshots: SnapshotNode[];
  viewMode: FileTreeViewMode;
}

const ACTION_CLASS =
  "shrink-0 cursor-pointer rounded-lg border border-edge px-3 py-1.5 text-sm text-slate-400 transition hover:border-edge-hover hover:text-ink disabled:cursor-not-allowed disabled:opacity-30";

const FileTreeToolbar = ({
  branches,
  checkedFileIds,
  currentSnapshotId,
  fileCount,
  onBranchChange,
  onClearSelection,
  onCollapseAll,
  onExpandAll,
  onScopeChange,
  onSearchChange,
  onSelectAll,
  onSnapshotChange,
  onViewModeChange,
  partitionCount,
  scope,
  search,
  selectedBranchName,
  snapshots,
  viewMode,
}: FileTreeToolbarProps) => {
  const [isCopied, setIsCopied] = useState(false);

  const handleCopyPaths = async () => {
    await navigator.clipboard.writeText([...checkedFileIds].join("\n"));
    setIsCopied(true);
    window.setTimeout(() => {
      setIsCopied(false);
    }, 2000);
  };

  return (
    <header className="flex shrink-0 flex-wrap items-center gap-2 border-b border-edge px-3 py-2 sm:px-6 sm:py-3">
      <FileTreeViewSettings
        branches={branches}
        currentSnapshotId={currentSnapshotId}
        onBranchChange={onBranchChange}
        onScopeChange={onScopeChange}
        onSnapshotChange={onSnapshotChange}
        onViewModeChange={onViewModeChange}
        scope={scope}
        selectedBranchName={selectedBranchName}
        snapshots={snapshots}
        viewMode={viewMode}
      />
      <div className="hidden h-5 w-px bg-edge sm:block" />
      <input
        type="search"
        aria-label="Search partitions"
        placeholder="Search partitions..."
        value={search}
        onChange={(event) => {
          onSearchChange(event.target.value);
        }}
        className="min-w-0 flex-1 rounded-lg border border-edge bg-surface px-3 py-1.5 text-sm text-ink placeholder:text-slate-500 focus:border-accent focus:outline-none sm:min-w-44 sm:max-w-xs"
      />
      <div className="flex w-full items-center gap-2 overflow-x-auto pb-1 sm:ml-auto sm:w-auto sm:flex-wrap sm:overflow-visible sm:pb-0">
        <button
          type="button"
          onClick={onExpandAll}
          disabled={partitionCount === 0}
          className={ACTION_CLASS}
        >
          Expand all
        </button>
        <button
          type="button"
          onClick={onCollapseAll}
          disabled={partitionCount === 0}
          className={ACTION_CLASS}
        >
          Collapse all
        </button>
        <button
          type="button"
          onClick={onSelectAll}
          disabled={fileCount === 0}
          className={ACTION_CLASS}
        >
          Select all
        </button>
        <button
          type="button"
          onClick={onClearSelection}
          disabled={checkedFileIds.size === 0}
          className={ACTION_CLASS}
        >
          Clear
        </button>
        <button
          type="button"
          onClick={() => void handleCopyPaths()}
          disabled={checkedFileIds.size === 0}
          className="shrink-0 cursor-pointer rounded-lg border border-accent px-3 py-1.5 text-sm text-accent transition hover:bg-accent-muted disabled:cursor-not-allowed disabled:border-edge disabled:text-slate-600"
        >
          {isCopied ? "Copied" : `Copy paths (${String(checkedFileIds.size)})`}
        </button>
        <span className="whitespace-nowrap text-xs text-slate-500">
          {partitionCount} partitions / {fileCount} files
        </span>
      </div>
    </header>
  );
};

export default FileTreeToolbar;
