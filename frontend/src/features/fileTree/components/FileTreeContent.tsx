import type { MouseEvent } from "react";
import type {
  DataFileNode,
  FileTreeFolder,
  FileTreeViewMode,
  PartitionGroup,
} from "../types";
import FileTreeFolderComponent from "./FileTreeFolder";
import FileTreePartition from "./FileTreePartition";

interface FileTreeContentProps {
  checkedFileIds: Set<string>;
  duplicatingNodeId: string | null;
  expandedItemIds: Set<string>;
  folders: FileTreeFolder[];
  inspectedFileId: string | null;
  inspectedFolderId: string | null;
  inspectedPartitionId: string | null;
  onCollapseMany: (folderIds: string[]) => void;
  onExpandMany: (folderIds: string[]) => void;
  onInspectFile: (file: DataFileNode) => void;
  onInspectFolder: (folder: FileTreeFolder) => void;
  onInspectPartition: (partition: PartitionGroup) => void;
  onToggleChecked: (fileId: string) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
  partitions: PartitionGroup[];
  search: string;
  viewMode: FileTreeViewMode;
}

const FileTreeContent = ({
  checkedFileIds,
  duplicatingNodeId,
  expandedItemIds,
  folders,
  inspectedFileId,
  inspectedFolderId,
  inspectedPartitionId,
  onCollapseMany,
  onExpandMany,
  onInspectFile,
  onInspectFolder,
  onInspectPartition,
  onToggleChecked,
  onToggleExpanded,
  onToggleFiles,
  onViewInGraph,
  partitions,
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
        {folders.map((folder) => (
          <FileTreeFolderComponent
            key={folder.id}
            checkedFileIds={checkedFileIds}
            depth={1}
            duplicatingNodeId={duplicatingNodeId}
            expandedItemIds={expandedItemIds}
            folder={folder}
            inspectedFileId={inspectedFileId}
            inspectedFolderId={inspectedFolderId}
            onCollapseMany={onCollapseMany}
            onExpandMany={onExpandMany}
            onInspectFile={onInspectFile}
            onInspectFolder={onInspectFolder}
            onToggleChecked={onToggleChecked}
            onToggleExpanded={onToggleExpanded}
            onToggleFiles={onToggleFiles}
            onViewInGraph={onViewInGraph}
          />
        ))}
      </div>
    </div>
  );
};

export default FileTreeContent;
