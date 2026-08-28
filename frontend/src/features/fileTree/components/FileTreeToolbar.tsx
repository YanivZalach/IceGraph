import { useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";

interface FileTreeToolbarProps {
  checkedFileIds: Set<string>;
  fileCount: number;
  onClearSelection: () => void;
  onCollapseAll: () => void;
  onExpandAll: () => void;
  onSearchChange: (search: string) => void;
  onSelectAll: () => void;
  partitionCount: number;
  search: string;
  settingsMenu: ReactNode;
}

const ACTION_CLASS =
  "shrink-0 cursor-pointer rounded-lg border border-edge px-3 py-1.5 text-sm text-slate-400 transition hover:border-edge-hover hover:text-ink disabled:cursor-not-allowed disabled:opacity-30";

const FileTreeToolbar = ({
  checkedFileIds,
  fileCount,
  onClearSelection,
  onCollapseAll,
  onExpandAll,
  onSearchChange,
  onSelectAll,
  partitionCount,
  search,
  settingsMenu,
}: FileTreeToolbarProps) => {
  const [copyStatus, setCopyStatus] = useState<"copied" | "failed" | "idle">(
    "idle",
  );
  const copyStatusTimeoutRef = useRef<number | null>(null);

  useEffect(
    () => () => {
      if (copyStatusTimeoutRef.current !== null) {
        window.clearTimeout(copyStatusTimeoutRef.current);
      }
    },
    [],
  );

  const handleCopyPaths = async () => {
    if (copyStatusTimeoutRef.current !== null) {
      window.clearTimeout(copyStatusTimeoutRef.current);
    }
    try {
      await navigator.clipboard.writeText([...checkedFileIds].join("\n"));
      setCopyStatus("copied");
    } catch {
      setCopyStatus("failed");
    }
    copyStatusTimeoutRef.current = window.setTimeout(() => {
      setCopyStatus("idle");
      copyStatusTimeoutRef.current = null;
    }, 2000);
  };

  return (
    <header className="flex shrink-0 flex-wrap items-center gap-2 border-b border-edge px-3 py-2 sm:px-6 sm:py-3">
      {settingsMenu}
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
          {copyStatus === "copied"
            ? "Copied"
            : copyStatus === "failed"
              ? "Copy failed"
              : `Copy paths (${String(checkedFileIds.size)})`}
        </button>
        <span className="whitespace-nowrap text-xs text-slate-500">
          {partitionCount} partitions / {fileCount} files
        </span>
      </div>
    </header>
  );
};

export default FileTreeToolbar;
