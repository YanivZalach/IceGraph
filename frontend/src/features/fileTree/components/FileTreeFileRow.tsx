import type { KeyboardEvent, MouseEvent } from "react";
import { fileTypeLabel } from "../../../graphConstants.js";
import { cn } from "../../../shared/lib/cn";
import type { DataFileNode } from "../types";

interface FileTreeFileRowProps {
  ariaLevel: number | undefined;
  duplicatingNodeId: string | null;
  file: DataFileNode;
  isChecked: boolean;
  isInspected: boolean;
  onInspect: () => void;
  onToggleChecked: () => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

const FileTreeFileRow = ({
  ariaLevel,
  duplicatingNodeId,
  file,
  isChecked,
  isInspected,
  onInspect,
  onToggleChecked,
  onViewInGraph,
}: FileTreeFileRowProps) => {
  const timestamp = file.details.earliest_appearing_snapshot_timestamp;
  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (
      event.target !== event.currentTarget ||
      (event.key !== "Enter" && event.key !== " ")
    ) {
      return;
    }
    event.preventDefault();
    onInspect();
  };

  return (
    <div
      role="listitem"
      aria-current={isInspected ? "true" : undefined}
      aria-description="Press Enter or Space to inspect"
      aria-level={ariaLevel}
      tabIndex={0}
      onKeyDown={handleKeyDown}
      onClick={onInspect}
      className={cn(
        "group flex cursor-pointer items-center gap-2.5 rounded-md border px-3 py-2 transition",
        isInspected
          ? "border-accent bg-accent-muted/60"
          : isChecked
            ? "border-accent/40 bg-accent-muted"
            : "border-transparent bg-canvas hover:border-edge hover:bg-surface-deep",
      )}
    >
      <input
        type="checkbox"
        aria-label={`Select ${file.id}`}
        checked={isChecked}
        onChange={onToggleChecked}
        onClick={(event) => {
          event.stopPropagation();
        }}
        className="size-3.5 shrink-0 cursor-pointer rounded accent-accent"
      />
      <div className="min-w-0 flex-1">
        <div
          className="overflow-hidden text-ellipsis whitespace-nowrap text-left font-mono text-xs text-slate-300"
          style={{
            direction: "rtl",
            textAlign: "left",
            textOverflow: "ellipsis",
          }}
          title={file.id}
        >
          {"\u202A" + file.id + "\u202C"}
        </div>
        <div className="mt-0.5 flex min-w-0 items-center gap-2 text-xs text-slate-600">
          <span className="shrink-0 text-caption font-semibold uppercase tracking-wide">
            {fileTypeLabel(file.type)}
          </span>
          {timestamp != null && (
            <span className="min-w-0 truncate whitespace-nowrap font-mono sm:hidden">
              {timestamp}
            </span>
          )}
        </div>
      </div>
      {timestamp != null && (
        <span className="hidden shrink-0 whitespace-nowrap font-mono text-xs text-slate-500 sm:block">
          {timestamp}
        </span>
      )}
      <button
        type="button"
        aria-label={`View ${file.id} in graph`}
        onClick={(event) => {
          onViewInGraph(event, file.id);
        }}
        disabled={duplicatingNodeId !== null}
        title={duplicatingNodeId === file.id ? "Opening..." : "View in graph"}
        className="ml-2 shrink-0 cursor-pointer rounded p-1 text-slate-500 transition-colors hover:bg-accent-muted hover:text-accent disabled:cursor-not-allowed disabled:opacity-50"
      >
        <svg
          className="size-3.5"
          viewBox="0 0 16 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.8"
          aria-hidden="true"
        >
          <circle cx="4" cy="8" r="2" />
          <circle cx="12" cy="4" r="2" />
          <circle cx="12" cy="12" r="2" />
          <path d="M6 7.2L10 4.8M6 8.8L10 11.2" strokeLinecap="round" />
        </svg>
      </button>
    </div>
  );
};

export default FileTreeFileRow;
