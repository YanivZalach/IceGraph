import type { MouseEvent } from "react";
import { buildVisibleFileTreeRows } from "../fileTreeRows";
import type { FileTreeRow } from "../fileTreeRows";
import type {
  DataFileNode,
  FileTreeViewMode,
  PartitionGroup,
  PartitionPathNode,
} from "../types";
import FileTreeIndentedRow from "./FileTreeIndentedRow";
import FileTreeFileRow from "./FileTreeFileRow";
import FileTreeGroupRow from "./FileTreeGroupRow";
import FileTreeVirtualList from "./FileTreeVirtualList";

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
  onToggleExpanded: (itemId: string) => void;
  onToggleChecked: (fileId: string) => void;
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
  onToggleExpanded,
  onToggleChecked,
  onToggleFiles,
  onViewInGraph,
  partitions,
  partitionPathNodes,
  search,
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
        <FileTreeFileRow
          ariaLevel={row.isHierarchical ? row.depth + 1 : undefined}
          checkedFileIds={checkedFileIds}
          duplicatingNodeId={duplicatingNodeId}
          file={row.file}
          isInspected={inspectedFileId === row.file.id}
          onInspect={onInspectFile}
          onToggleChecked={onToggleChecked}
          onViewInGraph={onViewInGraph}
        />
      ) : (
        <FileTreeGroupRow
          checkedFileIds={checkedFileIds}
          expandedItemIds={expandedItemIds}
          isInspected={
            row.kind === "partition"
              ? inspectedPartitionId === row.partition.id
              : inspectedPartitionPathNodeId === row.partitionPathNode.id
          }
          onCollapseMany={onCollapseMany}
          onExpandMany={onExpandMany}
          onInspectPartitionPathNode={onInspectPartitionPathNode}
          onInspectPartition={onInspectPartition}
          onToggleExpanded={onToggleExpanded}
          onToggleFiles={onToggleFiles}
          row={row}
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
    <FileTreeVirtualList renderRow={renderVisibleRow} rows={visibleRows} />
  );
};

export default FileTreeContent;
