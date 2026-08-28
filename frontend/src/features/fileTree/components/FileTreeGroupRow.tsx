import type { KeyboardEvent } from "react";
import { cn } from "../../../shared/lib/cn";
import type { FileTreeGroupRowModel } from "../model/fileTreeRows";
import { getLatestFileTimestamp } from "../model/partitionModel";

export type GroupCheckedState = "all" | "none" | "some";

interface FileTreeGroupRowProps {
  checkedState: GroupCheckedState;
  isExpanded: boolean;
  isInspected: boolean;
  onCollapseNested: () => void;
  onExpandNested: () => void;
  onInspect: () => void;
  onToggleChecked: () => void;
  onToggleExpanded: () => void;
  row: FileTreeGroupRowModel;
}

const NESTED_ACTION_CLASS =
  "flex size-8 cursor-pointer items-center justify-center rounded text-base text-slate-600 hover:bg-edge hover:text-slate-300";

const FileTreeGroupRow = ({
  checkedState,
  isExpanded,
  isInspected,
  onCollapseNested,
  onExpandNested,
  onInspect,
  onToggleChecked,
  onToggleExpanded,
  row,
}: FileTreeGroupRowProps) => {
  const latestTimestamp = getLatestFileTimestamp(row.files);
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
      aria-label={row.label}
      aria-level={row.ariaLevel}
      tabIndex={0}
      onClick={onInspect}
      onKeyDown={handleKeyDown}
      className={cn(
        "overflow-hidden rounded-lg border bg-surface",
        isInspected ? "border-accent" : "border-edge",
      )}
    >
      <div className="flex cursor-pointer items-center px-4 py-2.5 transition hover:bg-surface-hover">
        <div className="flex min-w-0 flex-1 items-center gap-3">
          <button
            type="button"
            aria-label={`${isExpanded ? "Collapse" : "Expand"} ${row.label}`}
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
          {row.isPartitionPath && (
            <svg
              className="size-4 shrink-0 text-slate-400"
              viewBox="0 0 20 20"
              fill="currentColor"
              aria-hidden="true"
            >
              <path d="M2 6a2 2 0 012-2h4l2 2h6a2 2 0 012 2v6a2 2 0 01-2 2H4a2 2 0 01-2-2V6z" />
            </svg>
          )}
          <span className="truncate font-mono text-xs text-slate-200">
            {row.label}
          </span>
        </div>
        <div className="ml-4 flex shrink-0 items-center gap-2">
          {latestTimestamp !== null && (
            <span className="hidden whitespace-nowrap font-mono text-xs text-slate-500 sm:block">
              {latestTimestamp}
            </span>
          )}
          <span className="rounded bg-edge px-2 py-0.5 text-xs font-semibold text-slate-300">
            {row.files.length}
          </span>
          {row.nestedGroupIds.length > 0 && (
            <>
              <button
                type="button"
                title="Expand nested partition paths"
                aria-label={`Expand every partition path inside ${row.label}`}
                onClick={(event) => {
                  event.stopPropagation();
                  onExpandNested();
                }}
                className={NESTED_ACTION_CLASS}
              >
                ⇊
              </button>
              <button
                type="button"
                title="Collapse nested partition paths"
                aria-label={`Collapse every partition path inside ${row.label}`}
                onClick={(event) => {
                  event.stopPropagation();
                  onCollapseNested();
                }}
                className={NESTED_ACTION_CLASS}
              >
                ⇈
              </button>
            </>
          )}
          <input
            type="checkbox"
            aria-label={`Select all files in ${row.label}`}
            checked={checkedState === "all"}
            ref={(element) => {
              if (element !== null) {
                element.indeterminate = checkedState === "some";
              }
            }}
            onChange={onToggleChecked}
            onClick={(event) => {
              event.stopPropagation();
            }}
            className="size-3.5 cursor-pointer rounded accent-accent"
          />
        </div>
      </div>
    </div>
  );
};

export default FileTreeGroupRow;
