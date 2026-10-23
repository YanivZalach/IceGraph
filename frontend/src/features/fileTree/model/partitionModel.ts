import type {
  DataFileNode,
  DataFileType,
  FileStatistics,
  PartitionGroup,
  PartitionPathNode,
} from "../types";
import { parseSignedInteger } from "../../../shared/components/statisticsTableBase";
import {
  formatPartitionFieldLabel,
  formatPartitionLabel,
  ROOT_PARTITION_LABEL,
} from "../../table/partitionLabel";

const UNPARTITIONED_NAME = "(unpartitioned)";

interface MutablePartitionPathNode {
  children: Map<string, MutablePartitionPathNode>;
  directFiles: DataFileNode[];
  keys: string[];
  label: string;
  path: string;
}

const countRecords = (
  files: DataFileNode[],
  type: DataFileType,
): bigint | null => {
  let total = 0n;
  for (const file of files) {
    if (file.type !== type) continue;
    const count = parseSignedInteger(file.details.row_count);
    if (count === undefined || count < 0n) return null;
    total += count;
  }
  return total;
};

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
    dataRowCount: countRecords(files, "data"),
    deletionVectorPositionCount: countRecords(files, "deletion_vector"),
    equalityDeleteRecordCount: countRecords(files, "equality_delete"),
    positionDeleteRecordCount: countRecords(files, "position_delete"),
    dataFileCount: files.filter((file) => file.type === "data").length,
    deletionVectorCount: files.filter((file) => file.type === "deletion_vector")
      .length,
    equalityDeleteFileCount: files.filter(
      (file) => file.type === "equality_delete",
    ).length,
    fileCount: files.length,
    hasCompleteFileSizes: fileSizes.length === files.length,
    largestSizeBytes: fileSizes.length === 0 ? 0 : Math.max(...fileSizes),
    positionDeleteFileCount: files.filter(
      (file) => file.type === "position_delete",
    ).length,
    smallestSizeBytes: fileSizes.length === 0 ? 0 : Math.min(...fileSizes),
    totalSizeBytes,
  };
};

export const getFileSizeBytes = (file: DataFileNode): number | null => {
  const rawFileSize =
    file.type === "deletion_vector"
      ? file.details.content_size_in_bytes
      : file.details.file_size_in_bytes;
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
  const partitionsByKey = new Map<string, PartitionGroup>();
  for (const file of files) {
    const partition = file.details.partition;
    const segments =
      partition === undefined
        ? []
        : partition.length === 0
          ? [{ key: ROOT_PARTITION_LABEL, label: ROOT_PARTITION_LABEL }]
          : partition.map((field) => ({
              key: JSON.stringify([field.name, field.field_id, field.value]),
              label: formatPartitionFieldLabel(field),
            }));
    const key =
      partition === undefined
        ? UNPARTITIONED_NAME
        : JSON.stringify(segments.map((segment) => segment.key));
    const existingPartition = partitionsByKey.get(key);
    if (existingPartition !== undefined) {
      existingPartition.files.push(file);
      continue;
    }
    const name =
      partition === undefined
        ? UNPARTITIONED_NAME
        : formatPartitionLabel(partition);
    partitionsByKey.set(key, {
      files: [file],
      id: `partition:${key}`,
      name,
      segments,
    });
  }

  const normalizedSearch = search.trim().toLowerCase();
  return [...partitionsByKey.values()]
    .filter(
      ({ name }) =>
        normalizedSearch === "" ||
        name.toLowerCase().includes(normalizedSearch),
    )
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
    id: `partition-path:${JSON.stringify(mutableNode.keys)}`,
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
    keys: [],
    label: "",
    path: "",
  };

  for (const partition of partitions) {
    if (partition.name === UNPARTITIONED_NAME) continue;
    let currentNode = root;
    for (const segment of partition.segments) {
      const path =
        currentNode.path === ""
          ? segment.label
          : `${currentNode.path}/${segment.label}`;
      const existingNode = currentNode.children.get(segment.key);
      if (existingNode !== undefined) {
        currentNode = existingNode;
        continue;
      }
      const newNode: MutablePartitionPathNode = {
        children: new Map(),
        directFiles: [],
        keys: [...currentNode.keys, segment.key],
        label: segment.label,
        path,
      };
      currentNode.children.set(segment.key, newNode);
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
