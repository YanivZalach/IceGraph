import type { ReactNode } from "react";
import { buildVisibleFileTreeRows } from "../fileTreeRows";
import type { FileTreeRow } from "../fileTreeRows";
import type {
  DataFileNode,
  FileTreeViewMode,
  PartitionGroup,
  PartitionPathNode,
} from "../types";
import FileTreeIndentedRow from "./FileTreeIndentedRow";
import FileTreePartition from "./FileTreePartition";
import FileTreeVirtualList from "./FileTreeVirtualList";
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
  const visibleRows = buildVisibleFileTreeRows(
    partitions,
    partitionPathNodes,
    expandedItemIds,
    viewMode,
  );
  const renderVisibleRow = (row: FileTreeRow) => (
    <FileTreeIndentedRow depth={row.depth}>
      {row.kind === "file" ? (
        renderFile(row.file, row.isTreeItem)
      ) : row.kind === "partition" ? (
        <FileTreePartition
          checkedFileIds={checkedFileIds}
          expandedItemIds={expandedItemIds}
          isInspected={inspectedPartitionId === row.partition.id}
          onInspectPartition={onInspectPartition}
          onToggleExpanded={onToggleExpanded}
          onToggleFiles={onToggleFiles}
          partition={row.partition}
        />
      ) : (
        <PartitionPathNodeComponent
          checkedFileIds={checkedFileIds}
          depth={row.depth + 1}
          expandedItemIds={expandedItemIds}
          inspectedPartitionPathNodeId={inspectedPartitionPathNodeId}
          onCollapseMany={onCollapseMany}
          onExpandMany={onExpandMany}
          onInspectPartitionPathNode={onInspectPartitionPathNode}
          onToggleExpanded={onToggleExpanded}
          onToggleFiles={onToggleFiles}
          partitionPathNode={row.partitionPathNode}
        />
      )}
    </FileTreeIndentedRow>
  );

  if (partitions.length === 0) {
    return (
      <div
        data-testid="file-tree-content-scroll"
        className="min-h-0 flex-1 overflow-y-auto"
      >
        <p className="mt-4 text-sm italic text-slate-500">
          {search === ""
            ? "No data files found for this snapshot and scope."
            : "No partitions match the search."}
        </p>
      </div>
    );
  }

  return (
    <FileTreeVirtualList
      renderRow={renderVisibleRow}
      rows={visibleRows}
      viewMode={viewMode}
    />
  );
};

export default FileTreeContent;
