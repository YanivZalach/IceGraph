import type { MouseEvent } from "react";
import { cn } from "../../../shared/lib/cn";
import type { DataFileNode } from "../fileTreeTypes";

interface FileTreeFileRowProps {
  checkedFileIds: Set<string>;
  duplicatingNodeId: string | null;
  file: DataFileNode;
  isInspected: boolean;
  isTreeItem: boolean;
  onInspect: (file: DataFileNode) => void;
  onToggleChecked: (fileId: string) => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

const FILE_TYPE_LABELS = {
  data: "Data",
  equality_delete: "Equality delete",
  position_delete: "Position delete",
} as const;

const FileTreeFileRow = ({
  checkedFileIds,
  duplicatingNodeId,
  file,
  isInspected,
  isTreeItem,
  onInspect,
  onToggleChecked,
  onViewInGraph,
}: FileTreeFileRowProps) => {
  const isChecked = checkedFileIds.has(file.id);
  const timestamp = file.details.earliest_appearing_snapshot_timestamp;

  return (
    <div
      role={isTreeItem ? "treeitem" : "listitem"}
      aria-selected={isInspected}
      onClick={() => {
        onInspect(file);
      }}
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
        onChange={() => {
          onToggleChecked(file.id);
        }}
        onClick={(event) => {
          event.stopPropagation();
        }}
        className="size-3.5 shrink-0 cursor-pointer rounded accent-accent"
      />
      <div className="min-w-0 flex-1">
        <div
          className="overflow-hidden text-ellipsis whitespace-nowrap text-left font-mono text-xs text-slate-300"
          dir="ltr"
          title={file.id}
        >
          {file.id}
        </div>
        <div className="mt-0.5 text-caption font-semibold uppercase tracking-wide text-slate-600">
          {FILE_TYPE_LABELS[file.type]}
        </div>
      </div>
      {timestamp != null && (
        <span className="shrink-0 whitespace-nowrap font-mono text-detail text-slate-500">
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
