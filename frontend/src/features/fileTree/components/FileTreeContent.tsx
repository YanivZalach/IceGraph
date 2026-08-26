import type { ReactNode } from "react";
import type {
  DataFileNode,
  FileTreeViewMode,
  PartitionGroup,
  PartitionPathNode,
} from "../types";
import FileTreePartition from "./FileTreePartition";
import PartitionPathNodeComponent from "./PartitionPathNode";

interface FileTreeContentProps {
  checkedFileIds: Set<string>;
  expandedItemIds: Set<string>;
  inspectedPartitionPathNodeId: string | null;
  inspectedPartitionId: string | null;
  onCollapseMany: (itemIds: string[]) => void;
  onExpandMany: (itemIds: string[]) => void;
  onInspectPartitionPathNode: (node: PartitionPathNode) => void;
  onInspectPartition: (partition: PartitionGroup) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  partitions: PartitionGroup[];
  partitionPathNodes: PartitionPathNode[];
  search: string;
  renderFile: (file: DataFileNode, isTreeItem: boolean) => ReactNode;
  viewMode: FileTreeViewMode;
}

const FileTreeContent = ({
  checkedFileIds,
  expandedItemIds,
  inspectedPartitionPathNodeId,
  inspectedPartitionId,
  onCollapseMany,
  onExpandMany,
  onInspectPartitionPathNode,
  onInspectPartition,
  onToggleExpanded,
  onToggleFiles,
  partitions,
  partitionPathNodes,
  search,
  renderFile,
  viewMode,
}: FileTreeContentProps) => {
  if (partitions.length === 0) {
    return (
      <p className="mt-4 text-sm italic text-slate-500">
        {search === ""
          ? "No data files found for this snapshot and scope."
          : "No partitions match the search."}
      </p>
    );
  }

  if (viewMode === "flat") {
    return (
      <div className="flex flex-col gap-2">
        {partitions.map((partition) => (
          <FileTreePartition
            key={partition.id}
            checkedFileIds={checkedFileIds}
            expandedItemIds={expandedItemIds}
            isInspected={inspectedPartitionId === partition.id}
            onInspectPartition={onInspectPartition}
            onToggleExpanded={onToggleExpanded}
            onToggleFiles={onToggleFiles}
            partition={partition}
            renderFile={renderFile}
          />
        ))}
      </div>
    );
  }

  const unpartitioned = partitions.find(
    (partition) => partition.name === "(unpartitioned)",
  );
  return (
    <div className="flex flex-col gap-2">
      {unpartitioned !== undefined && (
        <FileTreePartition
          checkedFileIds={checkedFileIds}
          expandedItemIds={expandedItemIds}
          isInspected={inspectedPartitionId === unpartitioned.id}
          onInspectPartition={onInspectPartition}
          onToggleExpanded={onToggleExpanded}
          onToggleFiles={onToggleFiles}
          partition={unpartitioned}
          renderFile={renderFile}
        />
      )}
      <div
        role="tree"
        aria-label="Data files by partition"
        className="flex flex-col gap-2"
      >
        {partitionPathNodes.map((partitionPathNode) => (
          <PartitionPathNodeComponent
            key={partitionPathNode.id}
            checkedFileIds={checkedFileIds}
            depth={1}
            expandedItemIds={expandedItemIds}
            inspectedPartitionPathNodeId={inspectedPartitionPathNodeId}
            onCollapseMany={onCollapseMany}
            onExpandMany={onExpandMany}
            onInspectPartitionPathNode={onInspectPartitionPathNode}
            onToggleExpanded={onToggleExpanded}
            onToggleFiles={onToggleFiles}
            partitionPathNode={partitionPathNode}
            renderFile={renderFile}
          />
        ))}
      </div>
    </div>
  );
};

export default FileTreeContent;
