import type { RefObject } from "react";
import type {
  ForceGraphMethods,
  LinkObject,
  NodeObject,
} from "react-force-graph-2d";
import { measureNode } from "../canvas/nodeMetrics";
import type { GraphLink, GraphNode } from "../model/graphModel";
import type { GraphViewport } from "./useGraphViewport";

export type GraphMethods = ForceGraphMethods<
  NodeObject<GraphNode>,
  LinkObject<GraphNode, GraphLink>
>;
export type GraphMethodsRef = RefObject<GraphMethods | undefined>;

const FIT_DURATION_MS = 500;
const FIT_PADDING_PX = 50;
const MAX_FIT_ZOOM = 1;

export interface GraphCamera {
  centerOn: (node: GraphNode, durationMs: number) => void;
  fitNodes: (nodeIds: ReadonlySet<string>) => void;
  resetZoom: () => void;
}

interface NodeBox {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
}

const restoreLayoutPositions = (nodes: GraphNode[]): void => {
  for (const node of nodes) {
    node.fx = node.layoutX;
    node.fy = node.layoutY;
    node.x = node.layoutX;
    node.y = node.layoutY;
    node.vx = 0;
    node.vy = 0;
  }
};

const measureContext = document.createElement("canvas").getContext("2d");

const boundingBox = (
  nodes: GraphNode[],
  viewport: GraphViewport,
): NodeBox | null => {
  if (nodes.length === 0 || measureContext === null) return null;
  const box = {
    minX: Infinity,
    minY: Infinity,
    maxX: -Infinity,
    maxY: -Infinity,
  };
  for (const node of nodes) {
    const { width, height } = measureNode(
      node,
      measureContext,
      viewport.nodeMetrics,
    );
    box.minX = Math.min(box.minX, node.layoutX - width / 2);
    box.maxX = Math.max(box.maxX, node.layoutX + width / 2);
    box.minY = Math.min(box.minY, node.layoutY - height / 2);
    box.maxY = Math.max(box.maxY, node.layoutY + height / 2);
  }
  return box;
};

export const useGraphCamera = (
  graphRef: GraphMethodsRef,
  nodes: GraphNode[],
  viewport: GraphViewport,
): GraphCamera => {
  const fitBox = (box: NodeBox | null): void => {
    const graph = graphRef.current;
    if (graph === undefined || box === null) return;
    const zoom = Math.min(
      MAX_FIT_ZOOM,
      Math.max(1, viewport.width - FIT_PADDING_PX * 2) /
        Math.max(1, box.maxX - box.minX),
      Math.max(1, viewport.height - FIT_PADDING_PX * 2) /
        Math.max(1, box.maxY - box.minY),
    );
    graph.centerAt(
      (box.minX + box.maxX) / 2,
      (box.minY + box.maxY) / 2,
      FIT_DURATION_MS,
    );
    graph.zoom(zoom, FIT_DURATION_MS);
  };

  return {
    centerOn: (node, durationMs) => {
      graphRef.current?.centerAt(
        node.fx ?? node.x,
        node.fy ?? node.y,
        durationMs,
      );
    },
    fitNodes: (nodeIds) => {
      fitBox(
        boundingBox(
          nodes.filter((node) => nodeIds.has(node.id)),
          viewport,
        ),
      );
    },
    resetZoom: () => {
      restoreLayoutPositions(nodes);
      fitBox(boundingBox(nodes, viewport));
    },
  };
};
