import { fileTypeLabel } from "../../graphConstants.js";
import type { FileTreeContext, GraphNode } from "./schemas";
import type {
  Branch,
  DataFileNode,
  FileTreeGraphIndex,
  SnapshotFileScope,
  SnapshotNode,
} from "./types";

interface BranchReference {
  "snapshot-id": string;
  type: string;
}

interface VersionedBranch extends Branch {
  timestamp: number;
}

interface CurrentSnapshotSelection {
  snapshot: SnapshotNode | undefined;
  warnings: string[];
}

interface SnapshotGraphTraversal {
  errors: string[];
  files: DataFileNode[];
  warnings: string[];
}

interface SnapshotLineageInspection {
  issueDescriptions: string[];
  nearestReadableParent: SnapshotGraphTraversal | undefined;
  nearestReadableParentId: string | undefined;
  skippedSnapshotIds: string[];
}

export interface SnapshotFileResult {
  errors: string[];
  files: DataFileNode[];
  warnings: string[];
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
  for (const node of context.nodes) {
    const connections = adjacencyByNodeId[node.id] ?? [];
    const connectedNodeIds = new Set(
      connections.map((connection) => connection.to),
    );
    const deletedChildIds = new Set(node.details.deleted_child_files ?? []);
    for (const childId of node.details.child_files ?? []) {
      if (connectedNodeIds.has(childId)) continue;
      connections.push({
        isDeleted: node.type === "manifest" && deletedChildIds.has(childId),
        to: childId,
      });
      connectedNodeIds.add(childId);
    }
    adjacencyByNodeId[node.id] = connections;
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

export const selectCurrentSnapshot = (
  snapshots: SnapshotNode[],
  requestedSnapshotId: string | null,
): CurrentSnapshotSelection => {
  if (requestedSnapshotId !== null) {
    const requestedSnapshot = snapshots.find(
      (snapshot) =>
        (snapshot.details.snapshot_id ?? snapshot.id) === requestedSnapshotId,
    );
    if (requestedSnapshot !== undefined) {
      return { snapshot: requestedSnapshot, warnings: [] };
    }
  }

  const latestSnapshot = snapshots.at(-1);
  if (requestedSnapshotId === null || latestSnapshot === undefined) {
    return { snapshot: latestSnapshot, warnings: [] };
  }
  const latestSnapshotId =
    latestSnapshot.details.snapshot_id ?? latestSnapshot.id;
  return {
    snapshot: latestSnapshot,
    warnings: [
      `Requested snapshot ${requestedSnapshotId} is unavailable in this branch or loaded range. Showing latest snapshot ${latestSnapshotId}.`,
    ],
  };
};

const traverseSnapshotGraph = (
  snapshot: SnapshotNode | undefined,
  graphIndex: FileTreeGraphIndex,
): SnapshotGraphTraversal => {
  if (snapshot === undefined) return { errors: [], files: [], warnings: [] };

  const errors: string[] = [];
  const filesById = new Map<string, DataFileNode>();
  const missingConnectionKeys = new Set<string>();
  const warnings: string[] = [];
  const visitedNodeIds = new Set<string>();
  const queuedNodeIds = [snapshot.id];

  while (queuedNodeIds.length > 0) {
    const currentNodeId = queuedNodeIds.shift();
    if (currentNodeId === undefined || visitedNodeIds.has(currentNodeId)) {
      continue;
    }
    visitedNodeIds.add(currentNodeId);
    const currentNode = graphIndex.nodesById[currentNodeId];
    if (currentNode === undefined) continue;

    if (currentNode.details.error) {
      errors.push(
        `${fileTypeLabel(currentNode.type)} (${currentNode.label ?? currentNode.id}): ${currentNode.details.error}`,
      );
    }

    for (const connection of graphIndex.adjacencyByNodeId[currentNodeId] ??
      []) {
      const child = graphIndex.nodesById[connection.to];
      if (child === undefined) {
        const connectionKey = `${currentNodeId}\u0000${connection.to}`;
        if (!missingConnectionKeys.has(connectionKey)) {
          missingConnectionKeys.add(connectionKey);
          warnings.push(
            `Graph node ${currentNodeId} references missing node ${connection.to}. Some files may be absent.`,
          );
        }
        continue;
      }
      if (isDataFileNode(child) && !connection.isDeleted) {
        filesById.set(child.id, child);
      }
      queuedNodeIds.push(child.id);
    }
  }

  return { errors, files: [...filesById.values()], warnings };
};

const getSnapshotIdentifier = (snapshot: SnapshotNode): string =>
  snapshot.details.snapshot_id ?? snapshot.id;

const inspectSnapshotLineage = (
  snapshot: SnapshotNode,
  graphIndex: FileTreeGraphIndex,
): SnapshotLineageInspection => {
  const issueDescriptions: string[] = [];
  let nearestReadableParent: SnapshotGraphTraversal | undefined;
  let nearestReadableParentId: string | undefined;
  const skippedSnapshotIds: string[] = [];
  const visitedSnapshotIds = new Set<string>();
  let parentSnapshotId = snapshot.details.parent_id;

  while (
    parentSnapshotId != null &&
    !visitedSnapshotIds.has(parentSnapshotId)
  ) {
    visitedSnapshotIds.add(parentSnapshotId);
    const parentSnapshot = graphIndex.snapshotsBySnapshotId[parentSnapshotId];
    if (parentSnapshot === undefined) break;

    const parentTraversal = traverseSnapshotGraph(parentSnapshot, graphIndex);
    const parentIssues = [
      ...parentTraversal.errors,
      ...parentTraversal.warnings,
    ];
    if (parentIssues.length > 0) {
      issueDescriptions.push(
        `Snapshot ${getSnapshotIdentifier(parentSnapshot)}: ${parentIssues.join(" ")}`,
      );
      if (nearestReadableParent === undefined) {
        skippedSnapshotIds.push(getSnapshotIdentifier(parentSnapshot));
      }
    } else if (nearestReadableParent === undefined) {
      nearestReadableParent = parentTraversal;
      nearestReadableParentId = getSnapshotIdentifier(parentSnapshot);
    }

    parentSnapshotId = parentSnapshot.details.parent_id;
  }

  return {
    issueDescriptions,
    nearestReadableParent,
    nearestReadableParentId,
    skippedSnapshotIds,
  };
};

const getLineageWarnings = (
  snapshot: SnapshotNode,
  lineage: SnapshotLineageInspection,
  scope: SnapshotFileScope,
): string[] => {
  if (lineage.issueDescriptions.length === 0) return [];

  const details = lineage.issueDescriptions.join("\n");
  if (scope === "snapshot") {
    return [
      `Snapshot ${getSnapshotIdentifier(snapshot)} has unreadable snapshots in its loaded history. The aggregated file tree may be incomplete.\n${details}`,
    ];
  }

  if (
    lineage.skippedSnapshotIds.length > 0 &&
    lineage.nearestReadableParentId !== undefined
  ) {
    return [
      `Added in commit compares snapshot ${getSnapshotIdentifier(snapshot)} with nearest readable snapshot ${lineage.nearestReadableParentId} because snapshot ${lineage.skippedSnapshotIds.join(
        ", ",
      )} could not be read. The result may include changes from multiple commits.\n${details}`,
    ];
  }

  return [
    `Snapshot ${getSnapshotIdentifier(snapshot)} has unreadable snapshots in its loaded history. Added in commit is based on the nearest readable parent, but the earlier history could not be fully verified.\n${details}`,
  ];
};

export const getSnapshotFileResult = (
  snapshot: SnapshotNode | undefined,
  graphIndex: FileTreeGraphIndex,
  scope: SnapshotFileScope,
): SnapshotFileResult => {
  const snapshotTraversal = traverseSnapshotGraph(snapshot, graphIndex);
  if (snapshot === undefined || snapshotTraversal.errors.length > 0) {
    return snapshotTraversal;
  }

  const lineage = inspectSnapshotLineage(snapshot, graphIndex);
  const lineageWarnings = getLineageWarnings(snapshot, lineage, scope);
  if (scope === "snapshot") {
    return {
      ...snapshotTraversal,
      warnings: [...snapshotTraversal.warnings, ...lineageWarnings],
    };
  }

  if (lineage.nearestReadableParent !== undefined) {
    const parentFileIds = new Set(
      lineage.nearestReadableParent.files.map(({ id }) => id),
    );
    return {
      errors: snapshotTraversal.errors,
      files: snapshotTraversal.files.filter(
        (file) => !parentFileIds.has(file.id),
      ),
      warnings: [...snapshotTraversal.warnings, ...lineageWarnings],
    };
  }

  const snapshotId = snapshot.details.snapshot_id;
  return {
    ...snapshotTraversal,
    files: snapshotTraversal.files.filter(
      (file) => file.details.earliest_appearing_snapshot_id === snapshotId,
    ),
    warnings: [...snapshotTraversal.warnings, ...lineageWarnings],
  };
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
