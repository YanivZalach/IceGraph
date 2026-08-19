import type { MouseEvent } from "react";
import { cn } from "../../../shared/lib/cn";
import { getAllPartitionPathNodeIds, getLatestFileTimestamp } from "../model";
import type {
  DataFileNode,
  PartitionPathNode as PartitionPathNodeData,
} from "../types";
import FileTreeFileRow from "./FileTreeFileRow";
import PartitionPathIcon from "./PartitionPathIcon";

interface PartitionPathNodeProps {
  checkedFileIds: Set<string>;
  depth: number;
  duplicatingNodeId: string | null;
  expandedItemIds: Set<string>;
  inspectedFileId: string | null;
  inspectedPartitionPathNodeId: string | null;
  onCollapseMany: (itemIds: string[]) => void;
  onExpandMany: (itemIds: string[]) => void;
  onInspectFile: (file: DataFileNode) => void;
  onInspectPartitionPathNode: (node: PartitionPathNodeData) => void;
  onToggleChecked: (fileId: string) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
  partitionPathNode: PartitionPathNodeData;
}

const PartitionPathNode = ({
  checkedFileIds,
  depth,
  duplicatingNodeId,
  expandedItemIds,
  inspectedFileId,
  inspectedPartitionPathNodeId,
  onCollapseMany,
  onExpandMany,
  onInspectFile,
  onInspectPartitionPathNode,
  onToggleChecked,
  onToggleExpanded,
  onToggleFiles,
  onViewInGraph,
  partitionPathNode,
}: PartitionPathNodeProps) => {
  const isExpanded = expandedItemIds.has(partitionPathNode.id);
  const isAllChecked =
    partitionPathNode.allFiles.length > 0 &&
    partitionPathNode.allFiles.every((file) => checkedFileIds.has(file.id));
  const isSomeChecked =
    !isAllChecked &&
    partitionPathNode.allFiles.some((file) => checkedFileIds.has(file.id));
  const descendantNodeIds = getAllPartitionPathNodeIds(
    partitionPathNode.children,
  );
  const latestTimestamp = getLatestFileTimestamp(partitionPathNode.allFiles);

  return (
    <div
      role="treeitem"
      aria-expanded={isExpanded}
      aria-level={depth}
      aria-selected={inspectedPartitionPathNodeId === partitionPathNode.id}
      className={cn(
        "overflow-hidden rounded-lg border bg-surface",
        inspectedPartitionPathNodeId === partitionPathNode.id
          ? "border-accent"
          : "border-edge",
      )}
    >
      <div
        onClick={() => {
          onInspectPartitionPathNode(partitionPathNode);
        }}
        className="flex cursor-pointer items-center px-4 py-2.5 transition hover:bg-surface-hover"
      >
        <div className="flex min-w-0 flex-1 items-center gap-3">
          <button
            type="button"
            aria-label={`${isExpanded ? "Collapse" : "Expand"} partition path ${partitionPathNode.label}`}
            onClick={(event) => {
              event.stopPropagation();
              onToggleExpanded(partitionPathNode.id);
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
          <PartitionPathIcon />
          <span className="truncate font-mono text-xs text-slate-200">
            {partitionPathNode.label}
          </span>
        </div>
        <div className="ml-4 flex shrink-0 items-center gap-2">
          {latestTimestamp !== null && (
            <span className="hidden whitespace-nowrap font-mono text-xs text-slate-500 sm:block">
              {latestTimestamp}
            </span>
          )}
          <span className="rounded bg-edge px-2 py-0.5 text-xs font-semibold text-slate-300">
            {partitionPathNode.allFiles.length}
          </span>
          {partitionPathNode.children.length > 0 && (
            <>
              <button
                type="button"
                title="Expand nested partition paths"
                aria-label={`Expand every partition path inside ${partitionPathNode.label}`}
                onClick={(event) => {
                  event.stopPropagation();
                  onExpandMany([partitionPathNode.id, ...descendantNodeIds]);
                }}
                className="flex size-8 cursor-pointer items-center justify-center rounded text-base text-slate-600 hover:bg-edge hover:text-slate-300"
              >
                ⇊
              </button>
              <button
                type="button"
                title="Collapse nested partition paths"
                aria-label={`Collapse every partition path inside ${partitionPathNode.label}`}
                onClick={(event) => {
                  event.stopPropagation();
                  onCollapseMany([partitionPathNode.id, ...descendantNodeIds]);
                }}
                className="flex size-8 cursor-pointer items-center justify-center rounded text-base text-slate-600 hover:bg-edge hover:text-slate-300"
              >
                ⇈
              </button>
            </>
          )}
          <input
            type="checkbox"
            aria-label={`Select all files in ${partitionPathNode.label}`}
            checked={isAllChecked}
            ref={(element) => {
              if (element !== null) element.indeterminate = isSomeChecked;
            }}
            onChange={() => {
              onToggleFiles(partitionPathNode.allFiles);
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
          role="group"
          className="flex flex-col gap-2 border-t border-edge px-4 py-2"
        >
          {partitionPathNode.children.map((child) => (
            <PartitionPathNode
              key={child.id}
              checkedFileIds={checkedFileIds}
              depth={depth + 1}
              duplicatingNodeId={duplicatingNodeId}
              expandedItemIds={expandedItemIds}
              inspectedFileId={inspectedFileId}
              inspectedPartitionPathNodeId={inspectedPartitionPathNodeId}
              onCollapseMany={onCollapseMany}
              onExpandMany={onExpandMany}
              onInspectFile={onInspectFile}
              onInspectPartitionPathNode={onInspectPartitionPathNode}
              onToggleChecked={onToggleChecked}
              onToggleExpanded={onToggleExpanded}
              onToggleFiles={onToggleFiles}
              onViewInGraph={onViewInGraph}
              partitionPathNode={child}
            />
          ))}
          {partitionPathNode.directFiles.map((file) => (
            <FileTreeFileRow
              key={file.id}
              checkedFileIds={checkedFileIds}
              duplicatingNodeId={duplicatingNodeId}
              file={file}
              isInspected={inspectedFileId === file.id}
              isTreeItem
              onInspect={onInspectFile}
              onToggleChecked={onToggleChecked}
              onViewInGraph={onViewInGraph}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export default PartitionPathNode;
