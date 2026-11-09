import type { GraphLink } from "./graphModel";

export type NeighborDirection = "parents" | "children";

export interface LineageAdjacency {
  parents: Map<string, Set<string>>;
  children: Map<string, Set<string>>;
}

const appendNeighbor = (
  neighbors: Map<string, Set<string>>,
  nodeId: string,
  neighborId: string,
): void => {
  const nodeNeighbors = neighbors.get(nodeId);
  if (nodeNeighbors === undefined) neighbors.set(nodeId, new Set([neighborId]));
  else nodeNeighbors.add(neighborId);
};

export const buildLineageAdjacency = (links: GraphLink[]): LineageAdjacency => {
  const adjacency: LineageAdjacency = {
    parents: new Map(),
    children: new Map(),
  };
  for (const link of links) {
    appendNeighbor(adjacency.children, link.source, link.target);
    appendNeighbor(adjacency.parents, link.target, link.source);
  }
  return adjacency;
};

export const getNeighborIds = (
  adjacency: LineageAdjacency,
  nodeId: string,
  direction: NeighborDirection,
): string[] => [...(adjacency[direction].get(nodeId) ?? [])];

const collectReachable = (
  neighbors: Map<string, Set<string>>,
  startId: string,
  reachable: Set<string>,
): void => {
  const visited = new Set([startId]);
  const pending = [startId];
  for (
    let nodeId = pending.pop();
    nodeId !== undefined;
    nodeId = pending.pop()
  ) {
    for (const neighborId of neighbors.get(nodeId) ?? []) {
      if (visited.has(neighborId)) continue;
      visited.add(neighborId);
      reachable.add(neighborId);
      pending.push(neighborId);
    }
  }
};

export const getLineage = (
  adjacency: LineageAdjacency,
  nodeId: string,
): Set<string> => {
  const lineage = new Set([nodeId]);
  collectReachable(adjacency.children, nodeId, lineage);
  collectReachable(adjacency.parents, nodeId, lineage);
  return lineage;
};
