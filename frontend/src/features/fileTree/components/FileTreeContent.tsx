import type { MouseEvent } from "react";
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
  duplicatingNodeId: string | null;
  expandedItemIds: Set<string>;
  inspectedFileId: string | null;
  inspectedPartitionPathNodeId: string | null;
  inspectedPartitionId: string | null;
  onCollapseMany: (itemIds: string[]) => void;
  onExpandMany: (itemIds: string[]) => void;
  onInspectFile: (file: DataFileNode) => void;
  onInspectPartitionPathNode: (node: PartitionPathNode) => void;
  onInspectPartition: (partition: PartitionGroup) => void;
  onToggleChecked: (fileId: string) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
  partitions: PartitionGroup[];
  partitionPathNodes: PartitionPathNode[];
  search: string;
  viewMode: FileTreeViewMode;
}

const FileTreeContent = ({
  checkedFileIds,
  duplicatingNodeId,
  expandedItemIds,
  inspectedFileId,
  inspectedPartitionPathNodeId,
  inspectedPartitionId,
  onCollapseMany,
  onExpandMany,
  onInspectFile,
  onInspectPartitionPathNode,
  onInspectPartition,
  onToggleChecked,
  onToggleExpanded,
  onToggleFiles,
  onViewInGraph,
  partitions,
  partitionPathNodes,
  search,
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
            duplicatingNodeId={duplicatingNodeId}
            expandedItemIds={expandedItemIds}
            inspectedFileId={inspectedFileId}
            isInspected={inspectedPartitionId === partition.id}
            onInspectFile={onInspectFile}
            onInspectPartition={onInspectPartition}
            onToggleChecked={onToggleChecked}
            onToggleExpanded={onToggleExpanded}
            onToggleFiles={onToggleFiles}
            onViewInGraph={onViewInGraph}
            partition={partition}
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
          duplicatingNodeId={duplicatingNodeId}
          expandedItemIds={expandedItemIds}
          inspectedFileId={inspectedFileId}
          isInspected={inspectedPartitionId === unpartitioned.id}
          onInspectFile={onInspectFile}
          onInspectPartition={onInspectPartition}
          onToggleChecked={onToggleChecked}
          onToggleExpanded={onToggleExpanded}
          onToggleFiles={onToggleFiles}
          onViewInGraph={onViewInGraph}
          partition={unpartitioned}
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
            partitionPathNode={partitionPathNode}
          />
        ))}
      </div>
    </div>
  );
};

export default FileTreeContent;
