import type { FileTreeContext, GraphNode } from "./schemas";
import type {
  Branch,
  DataFileNode,
  DataFileType,
  FileStatistics,
  FileTreeGraphIndex,
  PartitionGroup,
  PartitionPathNode,
  SnapshotFileScope,
  SnapshotNode,
} from "./types";
import { fileTypeLabel } from "../../graphConstants.js";
import { aggregateReadableMetrics } from "./readableMetrics";

const DATA_FILE_TYPES = new Set<DataFileType>([
  "data",
  "position_delete",
  "equality_delete",
]);

interface MutablePartitionPathNode {
  children: Map<string, MutablePartitionPathNode>;
  directFiles: DataFileNode[];
  label: string;
  path: string;
}

interface BranchReference {
  "snapshot-id": string;
  type: string;
}

interface VersionedBranch extends Branch {
  timestamp: number;
}

const isDataFileNode = (node: GraphNode): node is DataFileNode =>
  node.type === "data" ||
  node.type === "position_delete" ||
  node.type === "equality_delete";

const isSnapshotNode = (node: GraphNode): node is SnapshotNode =>
  node.type === "snapshot";

const getTimestampSortValue = (node: GraphNode): number => {
  const timestamp = node.details.timestamp;
  if (typeof timestamp === "number") return timestamp;
  if (typeof timestamp !== "string") return 0;
  const parsedTimestamp = Date.parse(timestamp);
  return Number.isNaN(parsedTimestamp) ? 0 : parsedTimestamp;
};

export const buildFileTreeGraphIndex = (
  context: FileTreeContext,
): FileTreeGraphIndex => {
  const nodesById: Record<string, GraphNode> = {};
  for (const node of context.nodes) nodesById[node.id] = node;

  const snapshots = context.nodes
    .filter(isSnapshotNode)
    .sort(
      (first, second) =>
        getTimestampSortValue(first) - getTimestampSortValue(second),
    );
  const snapshotsBySnapshotId: Record<string, SnapshotNode> = {};
  for (const snapshot of snapshots) {
    const snapshotId = snapshot.details.snapshot_id;
    if (snapshotId != null) snapshotsBySnapshotId[snapshotId] = snapshot;
  }

  const adjacencyByNodeId: FileTreeGraphIndex["adjacencyByNodeId"] = {};
  for (const edge of context.edges) {
    const connections = adjacencyByNodeId[edge.from] ?? [];
    connections.push({ isDeleted: edge.is_deleted === true, to: edge.to });
    adjacencyByNodeId[edge.from] = connections;
  }

  return {
    adjacencyByNodeId,
    nodesById,
    snapshots,
    snapshotsBySnapshotId,
  };
};

export const getBranches = (context: FileTreeContext): Branch[] => {
  const branchesByName = new Map<string, VersionedBranch>();
  const recordBranch = (
    name: string,
    reference: BranchReference,
    timestamp: number,
  ) => {
    if (reference.type !== "branch") return;
    const existing = branchesByName.get(name);
    if (existing !== undefined && existing.timestamp > timestamp) return;
    branchesByName.set(name, {
      headSnapshotId: reference["snapshot-id"],
      name,
      timestamp,
    });
  };

  for (const node of context.nodes) {
    if (node.type !== "metadata" && node.type !== "main_metadata") continue;
    for (const [name, reference] of Object.entries(node.details.refs ?? {})) {
      recordBranch(name, reference, getTimestampSortValue(node));
    }
  }
  for (const [name, reference] of Object.entries(
    context.metadata?.refs ?? {},
  )) {
    recordBranch(name, reference, Number.POSITIVE_INFINITY);
  }

  return [...branchesByName.values()]
    .map(({ headSnapshotId, name }) => ({ headSnapshotId, name }))
    .sort((first, second) => first.name.localeCompare(second.name));
};

export const getDisplayedSnapshots = (
  graphIndex: FileTreeGraphIndex,
  branches: Branch[],
  selectedBranchName: string | null,
): SnapshotNode[] => {
  if (selectedBranchName === null) return graphIndex.snapshots;
  const branch = branches.find(({ name }) => name === selectedBranchName);
  if (branch === undefined) return graphIndex.snapshots;

  const branchSnapshots: SnapshotNode[] = [];
  const visitedSnapshotIds = new Set<string>();
  let currentSnapshotId: string | null | undefined = branch.headSnapshotId;
  while (
    currentSnapshotId != null &&
    !visitedSnapshotIds.has(currentSnapshotId)
  ) {
    visitedSnapshotIds.add(currentSnapshotId);
    const snapshot: SnapshotNode | undefined =
      graphIndex.snapshotsBySnapshotId[currentSnapshotId];
    if (snapshot === undefined) break;
    branchSnapshots.push(snapshot);
    currentSnapshotId = snapshot.details.parent_id;
  }
  return branchSnapshots.reverse();
};

export const getCurrentSnapshot = (
  snapshots: SnapshotNode[],
  requestedSnapshotId: string | null,
): SnapshotNode | undefined => {
  if (requestedSnapshotId !== null) {
    const requestedSnapshot = snapshots.find(
      (snapshot) =>
        (snapshot.details.snapshot_id ?? snapshot.id) === requestedSnapshotId,
    );
    if (requestedSnapshot !== undefined) return requestedSnapshot;
  }
  return snapshots.at(-1);
};

const collectSnapshotFiles = (
  snapshot: SnapshotNode | undefined,
  graphIndex: FileTreeGraphIndex,
): DataFileNode[] => {
  if (snapshot === undefined) return [];

  const filesById = new Map<string, DataFileNode>();
  const visitedNodeIds = new Set<string>();
  const queuedNodeIds = [snapshot.id];
  while (queuedNodeIds.length > 0) {
    const currentNodeId = queuedNodeIds.shift();
    if (currentNodeId === undefined || visitedNodeIds.has(currentNodeId)) {
      continue;
    }
    visitedNodeIds.add(currentNodeId);

    for (const connection of graphIndex.adjacencyByNodeId[currentNodeId] ??
      []) {
      const child = graphIndex.nodesById[connection.to];
      if (child === undefined) continue;
      if (isDataFileNode(child)) {
        if (!connection.isDeleted && DATA_FILE_TYPES.has(child.type)) {
          filesById.set(child.id, child);
        }
      } else if (child.type === "manifest") {
        queuedNodeIds.push(child.id);
      }
    }
  }

  return [...filesById.values()];
};

export const getSnapshotFiles = (
  snapshot: SnapshotNode | undefined,
  graphIndex: FileTreeGraphIndex,
  scope: SnapshotFileScope,
): DataFileNode[] => {
  const snapshotFiles = collectSnapshotFiles(snapshot, graphIndex);
  if (scope === "snapshot") return snapshotFiles;
  if (snapshot === undefined) return [];

  const parentSnapshotId = snapshot.details.parent_id;
  const parentSnapshot =
    parentSnapshotId == null
      ? undefined
      : graphIndex.snapshotsBySnapshotId[parentSnapshotId];
  if (parentSnapshot !== undefined) {
    const parentFileIds = new Set(
      collectSnapshotFiles(parentSnapshot, graphIndex).map(({ id }) => id),
    );
    return snapshotFiles.filter((file) => !parentFileIds.has(file.id));
  }

  const snapshotId = snapshot.details.snapshot_id;
  return snapshotFiles.filter(
    (file) => file.details.earliest_appearing_snapshot_id === snapshotId,
  );
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

export const formatSnapshotVersion = (
  snapshot: SnapshotNode,
  isLatest: boolean,
): string => {
  const snapshotId = snapshot.details.snapshot_id ?? snapshot.id;
  const rawOperation = snapshot.details.operation_description;
  const operation =
    typeof rawOperation === "string" && rawOperation.trim() !== ""
      ? rawOperation
      : "Snapshot";
  const timestamp = snapshot.details.timestamp;
  const timestampLabel =
    typeof timestamp === "string" || typeof timestamp === "number"
      ? String(timestamp)
      : "Unknown time";
  return `ID ${snapshotId} · ${operation} · ${timestampLabel}${isLatest ? " · latest" : ""}`;
};

export const getLatestFileTimestamp = (files: DataFileNode[]): string | null =>
  files
    .map((file) => file.details.earliest_appearing_snapshot_timestamp)
    .filter((timestamp): timestamp is string => timestamp != null)
    .sort((first, second) => first.localeCompare(second))
    .at(-1) ?? null;

export const groupFilesByPartition = (
  files: DataFileNode[],
  search: string,
): PartitionGroup[] => {
  const filesByPartition = new Map<string, DataFileNode[]>();
  for (const file of files) {
    const partition = file.details.partition ?? "(unpartitioned)";
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
      statistics: calculateFileStatistics(partitionFiles),
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
    statistics: calculateFileStatistics(allFiles),
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

export const getSnapshotFileErrors = (
  snapshot: SnapshotNode | undefined,
  graphIndex: FileTreeGraphIndex,
): string[] => {
  if (snapshot === undefined) return [];
  const errors: string[] = [];
  const visitedNodeIds = new Set<string>();
  const queuedNodeIds = [snapshot.id];
  while (queuedNodeIds.length > 0) {
    const currentNodeId = queuedNodeIds.shift();
    if (currentNodeId === undefined || visitedNodeIds.has(currentNodeId)) {
      continue;
    }
    visitedNodeIds.add(currentNodeId);
    const node = graphIndex.nodesById[currentNodeId];
    if (node === undefined) continue;
    if (node.details.error) {
      errors.push(
        `${fileTypeLabel(node.type)} (${node.label ?? node.id}): ${node.details.error}`,
      );
    }
    for (const connection of graphIndex.adjacencyByNodeId[currentNodeId] ??
      []) {
      queuedNodeIds.push(connection.to);
    }
  }
  return errors;
};
