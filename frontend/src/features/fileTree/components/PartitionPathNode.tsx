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
    </div>
  );
};

export default PartitionPathNode;
