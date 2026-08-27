import type {
  DataFileNode,
  FileStatistics,
  PartitionGroup,
  PartitionPathNode,
} from "./types";
import { aggregateReadableMetrics } from "./aggregateReadableMetrics";

interface MutablePartitionPathNode {
  children: Map<string, MutablePartitionPathNode>;
  directFiles: DataFileNode[];
  label: string;
  path: string;
}

export const calculateFileStatistics = (
  files: DataFileNode[],
): FileStatistics => {
  const fileSizes = files
    .map(getFileSizeBytes)
    .filter((fileSize): fileSize is number => fileSize !== null);
  const totalSizeBytes = fileSizes.reduce((total, size) => total + size, 0);
  return {
    averageSizeBytes:
      fileSizes.length === 0 ? 0 : totalSizeBytes / fileSizes.length,
    dataFileCount: files.filter((file) => file.type === "data").length,
    equalityDeleteFileCount: files.filter(
      (file) => file.type === "equality_delete",
    ).length,
    fileCount: files.length,
    hasCompleteFileSizes: fileSizes.length === files.length,
    largestSizeBytes: fileSizes.length === 0 ? 0 : Math.max(...fileSizes),
    positionDeleteFileCount: files.filter(
      (file) => file.type === "position_delete",
    ).length,
    readableMetrics: aggregateReadableMetrics(files),
    smallestSizeBytes: fileSizes.length === 0 ? 0 : Math.min(...fileSizes),
    totalRowCount: files.reduce(
      (total, file) => total + (file.details.row_count ?? 0),
      0,
    ),
    totalSizeBytes,
  };
};

export const getFileSizeBytes = (file: DataFileNode): number | null => {
  const rawFileSize = file.details.file_size_in_bytes;
  if (rawFileSize === undefined) return null;
  const fileSizeBytes = Number(rawFileSize);
  return Number.isFinite(fileSizeBytes) && fileSizeBytes >= 0
    ? fileSizeBytes
    : null;
};

export const getLatestFileTimestamp = (files: DataFileNode[]): string | null =>
  files.reduce<string | null>((latestTimestamp, file) => {
    const timestamp = file.details.earliest_appearing_snapshot_timestamp;
    if (timestamp == null) return latestTimestamp;
    if (
      latestTimestamp === null ||
      timestamp.localeCompare(latestTimestamp) > 0
    )
      return timestamp;
    return latestTimestamp;
  }, null);

export const groupFilesByPartition = (
  files: DataFileNode[],
  search: string,
): PartitionGroup[] => {
  const filesByPartition = new Map<string, DataFileNode[]>();
  for (const file of files) {
    const rawPartition = file.details.partition;
    const partition =
      rawPartition === undefined || rawPartition.trim() === ""
        ? "(unpartitioned)"
        : rawPartition;
    const partitionFiles = filesByPartition.get(partition) ?? [];
    partitionFiles.push(file);
    filesByPartition.set(partition, partitionFiles);
  }

  const normalizedSearch = search.trim().toLowerCase();
  return [...filesByPartition.entries()]
    .filter(
      ([partition]) =>
        normalizedSearch === "" ||
        partition.toLowerCase().includes(normalizedSearch),
    )
    .map(([name, partitionFiles]) => ({
      files: partitionFiles,
      id: `partition:${name}`,
      name,
    }))
    .sort((first, second) => second.name.localeCompare(first.name));
};

const convertMutablePartitionPathNode = (
  mutableNode: MutablePartitionPathNode,
): PartitionPathNode => {
  const children = [...mutableNode.children.values()]
    .map(convertMutablePartitionPathNode)
    .sort((first, second) => second.label.localeCompare(first.label));
  const allFiles = [
    ...mutableNode.directFiles,
    ...children.flatMap((child) => child.allFiles),
  ];
  return {
    allFiles,
    children,
    directFiles: mutableNode.directFiles,
    id: `partition-path:${mutableNode.path}`,
    label: mutableNode.label,
    path: mutableNode.path,
  };
};

export const buildPartitionPathTree = (
  partitions: PartitionGroup[],
): PartitionPathNode[] => {
  const root: MutablePartitionPathNode = {
    children: new Map(),
    directFiles: [],
    label: "",
    path: "",
  };

  for (const partition of partitions) {
    if (partition.name === "(unpartitioned)") continue;
    let currentNode = root;
    for (const segment of partition.name.split(", ")) {
      const path =
        currentNode.path === "" ? segment : `${currentNode.path}/${segment}`;
      const existingNode = currentNode.children.get(segment);
      if (existingNode !== undefined) {
        currentNode = existingNode;
        continue;
      }
      const newNode: MutablePartitionPathNode = {
        children: new Map(),
        directFiles: [],
        label: segment,
        path,
      };
      currentNode.children.set(segment, newNode);
      currentNode = newNode;
    }
    currentNode.directFiles.push(...partition.files);
  }

  return [...root.children.values()]
    .map(convertMutablePartitionPathNode)
    .sort((first, second) => second.label.localeCompare(first.label));
};

export const getAllPartitionPathNodeIds = (
  nodes: PartitionPathNode[],
): string[] =>
  nodes.flatMap((node) => [
    node.id,
    ...getAllPartitionPathNodeIds(node.children),
  ]);
