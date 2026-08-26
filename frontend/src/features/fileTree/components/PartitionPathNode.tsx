import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import { getAllPartitionPathNodeIds } from "../partitionModel";
import type {
  DataFileNode,
  PartitionPathNode as PartitionPathNodeData,
} from "../types";
import FileTreeGroupHeader from "./FileTreeGroupHeader";
import PartitionPathIcon from "./PartitionPathIcon";

interface PartitionPathNodeProps {
  checkedFileIds: Set<string>;
  depth: number;
  expandedItemIds: Set<string>;
  inspectedPartitionPathNodeId: string | null;
  onCollapseMany: (itemIds: string[]) => void;
  onExpandMany: (itemIds: string[]) => void;
  onInspectPartitionPathNode: (node: PartitionPathNodeData) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  partitionPathNode: PartitionPathNodeData;
  renderFile: (file: DataFileNode, isTreeItem: boolean) => ReactNode;
}

const PartitionPathNode = ({
  checkedFileIds,
  depth,
  expandedItemIds,
  inspectedPartitionPathNodeId,
  onCollapseMany,
  onExpandMany,
  onInspectPartitionPathNode,
  onToggleExpanded,
  onToggleFiles,
  partitionPathNode,
  renderFile,
}: PartitionPathNodeProps) => {
  const isExpanded = expandedItemIds.has(partitionPathNode.id);
  const descendantNodeIds = getAllPartitionPathNodeIds(
    partitionPathNode.children,
  );

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
      <FileTreeGroupHeader
        accessibleLabel={`partition path ${partitionPathNode.label}`}
        additionalActions={
          partitionPathNode.children.length === 0 ? null : (
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
          )
        }
        checkedFileIds={checkedFileIds}
        files={partitionPathNode.allFiles}
        icon={<PartitionPathIcon />}
        isExpanded={isExpanded}
        label={partitionPathNode.label}
        onInspect={() => {
          onInspectPartitionPathNode(partitionPathNode);
        }}
        onToggleExpanded={() => {
          onToggleExpanded(partitionPathNode.id);
        }}
        onToggleFiles={onToggleFiles}
      />
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
              expandedItemIds={expandedItemIds}
              inspectedPartitionPathNodeId={inspectedPartitionPathNodeId}
              onCollapseMany={onCollapseMany}
              onExpandMany={onExpandMany}
              onInspectPartitionPathNode={onInspectPartitionPathNode}
              onToggleExpanded={onToggleExpanded}
              onToggleFiles={onToggleFiles}
              partitionPathNode={child}
              renderFile={renderFile}
            />
          ))}
          {partitionPathNode.directFiles.map((file) => renderFile(file, true))}
        </div>
      )}
    </div>
  );
};

export default PartitionPathNode;
