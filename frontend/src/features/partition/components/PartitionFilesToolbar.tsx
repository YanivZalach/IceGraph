import { useState } from "react";
import { copyText } from "../../../shared/lib/clipboard";

interface PartitionFilesToolbarProps {
  checkedFilePaths: string[];
  fileCount: number;
  onClearSelection: () => void;
  onCollapseAll: () => void;
  onExpandAll: () => void;
  onSearchChange: (search: string) => void;
  onSelectAll: () => void;
  search: string;
}

const ACTION_CLASS =
  "shrink-0 cursor-pointer rounded-lg border border-edge px-3 py-1.5 text-sm text-slate-400 transition hover:border-edge-hover hover:text-ink disabled:cursor-not-allowed disabled:opacity-30";
const COPY_STATUS_RESET_MS = 2000;

const PartitionFilesToolbar = ({
  checkedFilePaths,
  fileCount,
  onClearSelection,
  onCollapseAll,
  onExpandAll,
  onSearchChange,
  onSelectAll,
  search,
}: PartitionFilesToolbarProps) => {
  const [copyStatus, setCopyStatus] = useState<"copied" | "failed" | "idle">(
    "idle",
  );

  const handleCopyPaths = async () => {
    const isCopied = await copyText(checkedFilePaths.join("\n"));
    setCopyStatus(isCopied ? "copied" : "failed");
    window.setTimeout(() => {
      setCopyStatus("idle");
    }, COPY_STATUS_RESET_MS);
  };

  return (
    <header className="flex shrink-0 flex-col gap-2 border-b border-edge px-3 py-2 sm:px-6 sm:py-3">
      <div className="flex flex-wrap items-center gap-2">
        <input
          type="search"
          aria-label="Search files"
          placeholder="Search paths or partition values..."
          value={search}
          onChange={(event) => {
            onSearchChange(event.target.value);
          }}
          className="min-w-0 flex-1 rounded-lg border border-edge bg-surface px-3 py-1.5 text-sm text-ink placeholder:text-slate-500 focus:border-accent focus:outline-none sm:min-w-44 sm:max-w-sm"
        />
        <div className="flex w-full items-center gap-2 overflow-x-auto pb-1 sm:ml-auto sm:w-auto sm:flex-wrap sm:overflow-visible sm:pb-0">
          <button type="button" onClick={onExpandAll} className={ACTION_CLASS}>
            Expand all
          </button>
          <button
            type="button"
            onClick={onCollapseAll}
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
            disabled={checkedFilePaths.length === 0}
            className={ACTION_CLASS}
          >
            Clear
          </button>
          <button
            type="button"
            onClick={() => void handleCopyPaths()}
            disabled={checkedFilePaths.length === 0}
            className="shrink-0 cursor-pointer rounded-lg border border-accent px-3 py-1.5 text-sm text-accent transition hover:bg-accent-muted disabled:cursor-not-allowed disabled:border-edge disabled:text-slate-600"
          >
            {copyStatus === "copied"
              ? "Copied"
              : copyStatus === "failed"
                ? "Copy failed"
                : `Copy paths (${String(checkedFilePaths.length)})`}
          </button>
          <span className="whitespace-nowrap text-xs text-slate-500">
            {fileCount} files
          </span>
        </div>
      </div>
    </header>
  );
};

export default PartitionFilesToolbar;
