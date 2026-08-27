import type { KeyboardEvent } from "react";
import { cn } from "../../../shared/lib/cn";
import type { FileTreeRow } from "../fileTreeRows";
import {
  getAllPartitionPathNodeIds,
  getLatestFileTimestamp,
} from "../partitionModel";
import type { DataFileNode, PartitionGroup, PartitionPathNode } from "../types";
import PartitionPathIcon from "./PartitionPathIcon";

type FileTreeGroup = Exclude<FileTreeRow, { kind: "file" }>;

interface FileTreeGroupRowProps {
  checkedFileIds: Set<string>;
  expandedItemIds: Set<string>;
  isInspected: boolean;
  onCollapseMany: (itemIds: string[]) => void;
  onExpandMany: (itemIds: string[]) => void;
  onInspectPartitionPathNode: (node: PartitionPathNode) => void;
  onInspectPartition: (partition: PartitionGroup) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  row: FileTreeGroup;
}

const FileTreeGroupRow = ({
  checkedFileIds,
  expandedItemIds,
  isInspected,
  onCollapseMany,
  onExpandMany,
  onInspectPartitionPathNode,
  onInspectPartition,
  onToggleExpanded,
  onToggleFiles,
  row,
}: FileTreeGroupRowProps) => {
  const item = row.kind === "partition" ? row.partition : row.partitionPathNode;
  const files =
    row.kind === "partition"
      ? row.partition.files
      : row.partitionPathNode.allFiles;
  const label =
    row.kind === "partition" ? row.partition.name : row.partitionPathNode.label;
  const isExpanded = expandedItemIds.has(item.id);
  const isAllChecked =
    files.length > 0 && files.every((file) => checkedFileIds.has(file.id));
  const isSomeChecked =
    !isAllChecked && files.some((file) => checkedFileIds.has(file.id));
  const latestTimestamp = getLatestFileTimestamp(files);
  const handleInspect = () => {
    if (row.kind === "partition") onInspectPartition(row.partition);
    else onInspectPartitionPathNode(row.partitionPathNode);
  };
  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (
      event.target !== event.currentTarget ||
      (event.key !== "Enter" && event.key !== " ")
    ) {
      return;
    }
    event.preventDefault();
    handleInspect();
  };

  return (
    <div
      role="listitem"
      aria-current={isInspected ? "true" : undefined}
      aria-description="Press Enter or Space to inspect"
      aria-label={label}
      aria-level={row.isHierarchical ? row.depth + 1 : undefined}
      tabIndex={0}
      onClick={handleInspect}
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
            aria-label={`${isExpanded ? "Collapse" : "Expand"} ${label}`}
            aria-expanded={isExpanded}
            onClick={(event) => {
              event.stopPropagation();
              onToggleExpanded(item.id);
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
          {row.kind === "partition-path" && <PartitionPathIcon />}
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
          {row.kind === "partition-path" &&
            row.partitionPathNode.children.length > 0 && (
              <>
                <button
                  type="button"
                  title="Expand nested partition paths"
                  aria-label={`Expand every partition path inside ${label}`}
                  onClick={(event) => {
                    event.stopPropagation();
                    onExpandMany([
                      item.id,
                      ...getAllPartitionPathNodeIds(
                        row.partitionPathNode.children,
                      ),
                    ]);
                  }}
                  className="flex size-8 cursor-pointer items-center justify-center rounded text-base text-slate-600 hover:bg-edge hover:text-slate-300"
                >
                  ⇊
                </button>
                <button
                  type="button"
                  title="Collapse nested partition paths"
                  aria-label={`Collapse every partition path inside ${label}`}
                  onClick={(event) => {
                    event.stopPropagation();
                    onCollapseMany([
                      item.id,
                      ...getAllPartitionPathNodeIds(
                        row.partitionPathNode.children,
                      ),
                    ]);
                  }}
                  className="flex size-8 cursor-pointer items-center justify-center rounded text-base text-slate-600 hover:bg-edge hover:text-slate-300"
                >
                  ⇈
                </button>
              </>
            )}
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
    </div>
  );
};

export default FileTreeGroupRow;
