import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import { getLatestFileTimestamp } from "../partitionModel";
import type { DataFileNode } from "../types";

interface FileTreeGroupHeaderProps {
  accessibleLabel: string;
  additionalActions: ReactNode;
  checkedFileIds: Set<string>;
  files: DataFileNode[];
  icon: ReactNode;
  isExpanded: boolean;
  label: string;
  onInspect: () => void;
  onToggleExpanded: () => void;
  onToggleFiles: (files: DataFileNode[]) => void;
}

const FileTreeGroupHeader = ({
  accessibleLabel,
  additionalActions,
  checkedFileIds,
  files,
  icon,
  isExpanded,
  label,
  onInspect,
  onToggleExpanded,
  onToggleFiles,
}: FileTreeGroupHeaderProps) => {
  const isAllChecked =
    files.length > 0 && files.every((file) => checkedFileIds.has(file.id));
  const isSomeChecked =
    !isAllChecked && files.some((file) => checkedFileIds.has(file.id));
  const latestTimestamp = getLatestFileTimestamp(files);

  return (
    <div
      onClick={onInspect}
      className="flex cursor-pointer items-center px-4 py-2.5 transition hover:bg-surface-hover"
    >
      <div className="flex min-w-0 flex-1 items-center gap-3">
        <button
          type="button"
          aria-label={`${isExpanded ? "Collapse" : "Expand"} ${accessibleLabel}`}
          aria-expanded={isExpanded}
          onClick={(event) => {
            event.stopPropagation();
            onToggleExpanded();
          }}
          className={cn(
            "flex size-8 shrink-0 cursor-pointer items-center justify-center rounded text-accent transition hover:bg-accent-muted",
            isExpanded ? "" : "-rotate-90",
          )}
        >
          <svg
            className="size-4"
            viewBox="0 0 16 16"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.5"
            aria-hidden="true"
          >
            <path d="M4 6l4 4 4-4" strokeLinecap="round" />
          </svg>
        </button>
        {icon}
        <span className="truncate font-mono text-xs text-slate-200">
          {label}
        </span>
      </div>
      <div className="ml-4 flex shrink-0 items-center gap-2">
        {latestTimestamp !== null && (
          <span className="hidden whitespace-nowrap font-mono text-xs text-slate-500 sm:block">
            {latestTimestamp}
          </span>
        )}
        <span className="rounded bg-edge px-2 py-0.5 text-xs font-semibold text-slate-300">
          {files.length}
        </span>
        {additionalActions}
        <input
          type="checkbox"
          aria-label={`Select all files in ${label}`}
          checked={isAllChecked}
          ref={(element) => {
            if (element !== null) element.indeterminate = isSomeChecked;
          }}
          onChange={() => {
            onToggleFiles(files);
          }}
          onClick={(event) => {
            event.stopPropagation();
          }}
          className="size-3.5 cursor-pointer rounded accent-accent"
        />
      </div>
    </div>
  );
};

export default FileTreeGroupHeader;
