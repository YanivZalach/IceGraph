import type {
  DataFileNode,
  FileTreeViewMode,
  InspectedFileTreeItem,
  PartitionGroup,
  PartitionPathNode,
} from "../types";

export interface FileTreeFileRowModel {
  ariaLevel: number | undefined;
  depth: number;
  file: DataFileNode;
  id: string;
  kind: "file";
}

export interface FileTreeGroupRowModel {
  ariaLevel: number | undefined;
  depth: number;
  files: DataFileNode[];
  id: string;
  inspectionTarget: InspectedFileTreeItem;
  isPartitionPath: boolean;
  kind: "group";
  label: string;
  nestedGroupIds: string[];
}

export type FileTreeRow = FileTreeFileRowModel | FileTreeGroupRowModel;

export type GroupCheckedState = "all" | "none" | "some";

export const getGroupCheckedState = (
  files: DataFileNode[],
  checkedFileIds: Set<string>,
): GroupCheckedState => {
  if (files.length === 0) return "none";
  if (files.every((file) => checkedFileIds.has(file.id))) return "all";
  return files.some((file) => checkedFileIds.has(file.id)) ? "some" : "none";
};

const createFileRow = (
  file: DataFileNode,
  depth: number,
  isTreeMode: boolean,
): FileTreeFileRowModel => ({
  ariaLevel: isTreeMode ? depth + 1 : undefined,
  depth,
  file,
  id: `file:${file.id}`,
  kind: "file",
});

const createPartitionRow = (
  partition: PartitionGroup,
  isTreeMode: boolean,
): FileTreeGroupRowModel => ({
  ariaLevel: isTreeMode ? 1 : undefined,
  depth: 0,
  files: partition.files,
  id: partition.id,
  inspectionTarget: { kind: "partition", partition },
  isPartitionPath: false,
  kind: "group",
  label: partition.name,
  nestedGroupIds: [],
});

const createPartitionPathRow = (
  partitionPathNode: PartitionPathNode,
  depth: number,
): FileTreeGroupRowModel => ({
  ariaLevel: depth + 1,
  depth,
  files: partitionPathNode.allFiles,
  id: partitionPathNode.id,
  inspectionTarget: { kind: "partition-path", partitionPathNode },
  isPartitionPath: true,
  kind: "group",
  label: partitionPathNode.label,
  nestedGroupIds: partitionPathNode.descendantIds,
});

const appendPartitionPathRows = (
  rows: FileTreeRow[],
  partitionPathNode: PartitionPathNode,
  depth: number,
  expandedItemIds: Set<string>,
) => {
  rows.push(createPartitionPathRow(partitionPathNode, depth));
  if (!expandedItemIds.has(partitionPathNode.id)) return;

  for (const child of partitionPathNode.children) {
    appendPartitionPathRows(rows, child, depth + 1, expandedItemIds);
  }
  for (const file of partitionPathNode.directFiles) {
    rows.push(createFileRow(file, depth + 1, true));
  }
};

const appendPartitionRows = (
  rows: FileTreeRow[],
  partition: PartitionGroup,
  expandedItemIds: Set<string>,
  isTreeMode: boolean,
) => {
  rows.push(createPartitionRow(partition, isTreeMode));
  if (!expandedItemIds.has(partition.id)) return;

  for (const file of partition.files) {
    rows.push(createFileRow(file, 1, isTreeMode));
  }
};

export const buildVisibleFileTreeRows = (
  partitions: PartitionGroup[],
  partitionPathNodes: PartitionPathNode[],
  expandedItemIds: Set<string>,
  viewMode: FileTreeViewMode,
): FileTreeRow[] => {
  const rows: FileTreeRow[] = [];
  if (viewMode === "flat") {
    for (const partition of partitions) {
      appendPartitionRows(rows, partition, expandedItemIds, false);
    }
    return rows;
  }

  const unpartitioned = partitions.find(
    (partition) => partition.name === "(unpartitioned)",
  );
  if (unpartitioned !== undefined) {
    appendPartitionRows(rows, unpartitioned, expandedItemIds, true);
  }
  for (const partitionPathNode of partitionPathNodes) {
    appendPartitionPathRows(rows, partitionPathNode, 0, expandedItemIds);
  }
  return rows;
};

export const getInspectedRowId = (
  inspectedItem: InspectedFileTreeItem | null,
): string | null => {
  if (inspectedItem === null) return null;
  if (inspectedItem.kind === "file") return `file:${inspectedItem.file.id}`;
  if (inspectedItem.kind === "partition") return inspectedItem.partition.id;
  return inspectedItem.partitionPathNode.id;
};
