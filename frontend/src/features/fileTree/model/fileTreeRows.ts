import type {
  DataFileNode,
  FileTreeViewMode,
  PartitionGroup,
  PartitionPathNode,
} from "../types";

export type FileTreeRow =
  | {
      depth: number;
      file: DataFileNode;
      id: string;
      isHierarchical: boolean;
      kind: "file";
    }
  | {
      depth: number;
      id: string;
      isHierarchical: true;
      kind: "partition-path";
      partitionPathNode: PartitionPathNode;
    }
  | {
      depth: number;
      id: string;
      isHierarchical: boolean;
      kind: "partition";
      partition: PartitionGroup;
    };

const appendPartitionPathRows = (
  rows: FileTreeRow[],
  partitionPathNode: PartitionPathNode,
  depth: number,
  expandedItemIds: Set<string>,
) => {
  rows.push({
    depth,
    id: partitionPathNode.id,
    isHierarchical: true,
    kind: "partition-path",
    partitionPathNode,
  });
  if (!expandedItemIds.has(partitionPathNode.id)) return;

  for (const child of partitionPathNode.children) {
    appendPartitionPathRows(rows, child, depth + 1, expandedItemIds);
  }
  for (const file of partitionPathNode.directFiles) {
    rows.push({
      depth: depth + 1,
      file,
      id: `file:${file.id}`,
      isHierarchical: true,
      kind: "file",
    });
  }
};

const appendPartitionRows = (
  rows: FileTreeRow[],
  partition: PartitionGroup,
  expandedItemIds: Set<string>,
  isHierarchical: boolean,
) => {
  rows.push({
    depth: 0,
    id: partition.id,
    isHierarchical,
    kind: "partition",
    partition,
  });
  if (!expandedItemIds.has(partition.id)) return;

  for (const file of partition.files) {
    rows.push({
      depth: 1,
      file,
      id: `file:${file.id}`,
      isHierarchical,
      kind: "file",
    });
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
