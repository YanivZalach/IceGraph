import { fileTypeLabel } from "../../../graphConstants.js";
import type {
  DataFileNode,
  FileTreeGraphIndex,
  SnapshotFileScope,
  SnapshotNode,
} from "../types";
import { isDataFileNode } from "./graphIndex";

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

const traverseSnapshotGraph = (
  snapshot: SnapshotNode | undefined,
  graphIndex: FileTreeGraphIndex,
  shouldCollectFiles = true,
): SnapshotGraphTraversal => {
  if (snapshot === undefined) return { errors: [], files: [], warnings: [] };

  const errors: string[] = [];
  const filesById = new Map<string, DataFileNode>();
  const missingConnectionKeys = new Set<string>();
  const warnings: string[] = [];
  const visitedNodeIds = new Set<string>();
  const queuedNodeIds = [snapshot.id];
  let queuedNodeIndex = 0;

  while (queuedNodeIndex < queuedNodeIds.length) {
    const currentNodeId = queuedNodeIds[queuedNodeIndex];
    queuedNodeIndex += 1;
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
      if (
        shouldCollectFiles &&
        isDataFileNode(child) &&
        !connection.isDeleted
      ) {
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
  scope: SnapshotFileScope,
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

    const shouldCollectParentFiles =
      scope === "commit" && nearestReadableParent === undefined;
    const parentTraversal = traverseSnapshotGraph(
      parentSnapshot,
      graphIndex,
      shouldCollectParentFiles,
    );
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
    } else if (
      shouldCollectParentFiles &&
      nearestReadableParent === undefined
    ) {
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

  const lineage = inspectSnapshotLineage(snapshot, graphIndex, scope);
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
