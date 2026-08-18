import type { MouseEvent } from "react";
import { cn } from "../../../shared/lib/cn";
import { getLatestFileTimestamp } from "../fileTreeModel";
import type { DataFileNode, PartitionGroup } from "../fileTreeTypes";
import FileTreeFileRow from "./FileTreeFileRow";

interface FileTreePartitionProps {
  checkedFileIds: Set<string>;
  duplicatingNodeId: string | null;
  expandedItemIds: Set<string>;
  inspectedFileId: string | null;
  isInspected: boolean;
  onInspectFile: (file: DataFileNode) => void;
  onInspectPartition: (partition: PartitionGroup) => void;
  onToggleChecked: (fileId: string) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
  partition: PartitionGroup;
}

const FileTreePartition = ({
  checkedFileIds,
  duplicatingNodeId,
  expandedItemIds,
  inspectedFileId,
  isInspected,
  onInspectFile,
  onInspectPartition,
  onToggleChecked,
  onToggleExpanded,
  onToggleFiles,
  onViewInGraph,
  partition,
}: FileTreePartitionProps) => {
  const isExpanded = expandedItemIds.has(partition.id);
  const isAllChecked =
    partition.files.length > 0 &&
    partition.files.every((file) => checkedFileIds.has(file.id));
  const isSomeChecked =
    !isAllChecked &&
    partition.files.some((file) => checkedFileIds.has(file.id));
  const latestTimestamp = getLatestFileTimestamp(partition.files);

  return (
    <section
      aria-label={partition.name}
      className={cn(
        "overflow-hidden rounded-lg border bg-surface",
        isInspected ? "border-accent" : "border-edge",
      )}
    >
      <div
        onClick={() => {
          onInspectPartition(partition);
        }}
        className="flex cursor-pointer items-center px-4 py-2.5 transition hover:bg-surface-hover"
      >
        <div className="flex min-w-0 flex-1 items-center gap-3">
          <button
            type="button"
            aria-label={`${isExpanded ? "Collapse" : "Expand"} ${partition.name}`}
            aria-expanded={isExpanded}
            onClick={(event) => {
              event.stopPropagation();
              onToggleExpanded(partition.id);
            }}
            className={cn(
              "shrink-0 cursor-pointer text-accent transition-transform",
              isExpanded ? "" : "-rotate-90",
            )}
          >
            <svg
              className="size-3.5"
              viewBox="0 0 16 16"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.5"
              aria-hidden="true"
            >
              <path d="M4 6l4 4 4-4" strokeLinecap="round" />
            </svg>
          </button>
          <span className="truncate font-mono text-xs text-slate-200">
            {partition.name}
          </span>
        </div>
        <div className="ml-4 flex shrink-0 items-center gap-3">
          {latestTimestamp !== null && (
            <span className="whitespace-nowrap font-mono text-detail text-slate-500">
              {latestTimestamp}
            </span>
          )}
          <span className="rounded bg-edge px-2 py-0.5 text-xs font-semibold text-slate-300">
            {partition.files.length}
          </span>
          <input
            type="checkbox"
            aria-label={`Select all files in ${partition.name}`}
            checked={isAllChecked}
            ref={(element) => {
              if (element !== null) element.indeterminate = isSomeChecked;
            }}
            onChange={() => {
              onToggleFiles(partition.files);
            }}
            onClick={(event) => {
              event.stopPropagation();
            }}
            className="size-3.5 cursor-pointer rounded accent-accent"
          />
        </div>
      </div>
      {isExpanded && (
        <div
          role="list"
          className="flex flex-col gap-1 border-t border-edge px-4 py-2"
        >
          {partition.files.map((file) => (
            <FileTreeFileRow
              key={file.id}
              checkedFileIds={checkedFileIds}
              duplicatingNodeId={duplicatingNodeId}
              file={file}
              isInspected={inspectedFileId === file.id}
              isTreeItem={false}
              onInspect={onInspectFile}
              onToggleChecked={onToggleChecked}
              onViewInGraph={onViewInGraph}
            />
          ))}
        </div>
      )}
    </section>
  );
};

export default FileTreePartition;
