import type { FileTreeContext, GraphNode } from "./fileTreeSchemas";
import type {
  Branch,
  DataFileNode,
  DataFileType,
  FileStatistics,
  FileTreeFolder,
  FileTreeGraphIndex,
  PartitionGroup,
  SnapshotFileScope,
  SnapshotNode,
} from "./fileTreeTypes";

const BYTES_PER_GIBIBYTE = 1024 ** 3;
const DATA_FILE_TYPES = new Set<DataFileType>([
  "data",
  "position_delete",
  "equality_delete",
]);

interface MutableFileTreeFolder {
  children: Map<string, MutableFileTreeFolder>;
  directFiles: DataFileNode[];
  label: string;
  path: string;
}

const isDataFileNode = (node: GraphNode): node is DataFileNode =>
  node.type === "data" ||
  node.type === "position_delete" ||
  node.type === "equality_delete";

const isSnapshotNode = (node: GraphNode): node is SnapshotNode =>
  node.type === "snapshot";

const getTimestampSortValue = (snapshot: SnapshotNode): number => {
  const timestamp = snapshot.details.timestamp;
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

export const getBranches = (context: FileTreeContext): Branch[] =>
  Object.entries(context.metadata?.refs ?? {})
    .filter(([, reference]) => reference.type === "branch")
    .map(([name, reference]) => ({
      headSnapshotId: reference["snapshot-id"],
      name,
    }))
    .sort((first, second) => first.name.localeCompare(second.name));

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
      (snapshot) => snapshot.details.snapshot_id === requestedSnapshotId,
    );
    if (requestedSnapshot !== undefined) return requestedSnapshot;
  }
  return snapshots.at(-1);
};

export const getSnapshotFiles = (
  snapshot: SnapshotNode | undefined,
  graphIndex: FileTreeGraphIndex,
  scope: SnapshotFileScope,
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

  const snapshotFiles = [...filesById.values()];
  if (scope === "snapshot") return snapshotFiles;
  const snapshotId = snapshot.details.snapshot_id;
  return snapshotFiles.filter(
    (file) => file.details.earliest_appearing_snapshot_id === snapshotId,
  );
};

export const calculateFileStatistics = (
  files: DataFileNode[],
): FileStatistics => {
  const fileSizes = files.map(
    (file) => (file.details.size_gb ?? 0) * BYTES_PER_GIBIBYTE,
  );
  const totalSizeBytes = fileSizes.reduce((total, size) => total + size, 0);
  return {
    averageSizeBytes: files.length === 0 ? 0 : totalSizeBytes / files.length,
    dataFileCount: files.filter((file) => file.type === "data").length,
    equalityDeleteFileCount: files.filter(
      (file) => file.type === "equality_delete",
    ).length,
    fileCount: files.length,
    largestSizeBytes: fileSizes.length === 0 ? 0 : Math.max(...fileSizes),
    positionDeleteFileCount: files.filter(
      (file) => file.type === "position_delete",
    ).length,
    smallestSizeBytes: fileSizes.length === 0 ? 0 : Math.min(...fileSizes),
    totalRowCount: files.reduce(
      (total, file) => total + (file.details.row_count ?? 0),
      0,
    ),
    totalSizeBytes,
  };
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

const convertMutableFolder = (
  mutableFolder: MutableFileTreeFolder,
): FileTreeFolder => {
  const children = [...mutableFolder.children.values()]
    .map(convertMutableFolder)
    .sort((first, second) => second.label.localeCompare(first.label));
  const allFiles = [
    ...mutableFolder.directFiles,
    ...children.flatMap((child) => child.allFiles),
  ];
  return {
    allFiles,
    children,
    directFiles: mutableFolder.directFiles,
    id: `folder:${mutableFolder.path}`,
    label: mutableFolder.label,
    path: mutableFolder.path,
    statistics: calculateFileStatistics(allFiles),
  };
};

export const buildFileTree = (
  partitions: PartitionGroup[],
): FileTreeFolder[] => {
  const root: MutableFileTreeFolder = {
    children: new Map(),
    directFiles: [],
    label: "",
    path: "",
  };

  for (const partition of partitions) {
    if (partition.name === "(unpartitioned)") continue;
    let currentFolder = root;
    for (const segment of partition.name.split(", ")) {
      const path =
        currentFolder.path === ""
          ? segment
          : `${currentFolder.path}/${segment}`;
      const existingFolder = currentFolder.children.get(segment);
      if (existingFolder !== undefined) {
        currentFolder = existingFolder;
        continue;
      }
      const newFolder: MutableFileTreeFolder = {
        children: new Map(),
        directFiles: [],
        label: segment,
        path,
      };
      currentFolder.children.set(segment, newFolder);
      currentFolder = newFolder;
    }
    currentFolder.directFiles.push(...partition.files);
  }

  return [...root.children.values()]
    .map(convertMutableFolder)
    .sort((first, second) => second.label.localeCompare(first.label));
};

export const getAllFolderIds = (folders: FileTreeFolder[]): string[] =>
  folders.flatMap((folder) => [folder.id, ...getAllFolderIds(folder.children)]);

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
        `${node.type} (${node.label ?? node.id}): ${node.details.error}`,
      );
    }
    for (const connection of graphIndex.adjacencyByNodeId[currentNodeId] ??
      []) {
      queuedNodeIds.push(connection.to);
    }
  }
  return errors;
};

export const formatByteSize = (sizeBytes: number): string => {
  if (sizeBytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const unitIndex = Math.min(
    Math.floor(Math.log(sizeBytes) / Math.log(1024)),
    units.length - 1,
  );
  const unit = units[unitIndex];
  if (unit === undefined) return `${String(Math.round(sizeBytes))} B`;
  const value = sizeBytes / 1024 ** unitIndex;
  return `${value.toLocaleString(undefined, { maximumFractionDigits: 2 })} ${unit}`;
};
