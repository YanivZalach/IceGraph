import type { FileTreeContext, GraphNode } from "../schemas";
import type { DataFileNode, FileTreeGraphIndex, SnapshotNode } from "../types";

export const isDataFileNode = (node: GraphNode): node is DataFileNode =>
  node.type === "data" ||
  node.type === "position_delete" ||
  node.type === "equality_delete";

const isSnapshotNode = (node: GraphNode): node is SnapshotNode =>
  node.type === "snapshot";

export const getTimestampSortValue = (node: GraphNode): number => {
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
