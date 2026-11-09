import type { GraphNode } from "../model/graphModel";
import { isBoxInFrame, isTextReadable, type FrameBounds } from "./frameBounds";
import {
  cachedNodeSize,
  measureNode,
  nodeLabel,
  type GraphNodeMetrics,
} from "./nodeMetrics";

const NODE_BORDER_COLOR = "#ffffff";
const CATALOG_TEXT_COLOR = "#0f172a";
const NODE_TEXT_COLOR = "#ffffff";
const NODE_TEXT_OUTLINE_COLOR = "#000000";
const CURRENT_METADATA_BORDER_DASH = [24, 16];

const traceNodeShape = (
  context: CanvasRenderingContext2D,
  x: number,
  y: number,
  width: number,
  height: number,
): void => {
  context.beginPath();
  context.roundRect(x, y, width, height, 4);
};

const strokeNodeBorder = (
  node: GraphNode,
  context: CanvasRenderingContext2D,
): void => {
  context.strokeStyle = NODE_BORDER_COLOR;
  context.lineWidth = node.kind === "catalog" ? 6 : 2;
  context.setLineDash(
    node.kind === "current_metadata" ? CURRENT_METADATA_BORDER_DASH : [],
  );
  context.stroke();
  context.setLineDash([]);
};

const fillNodeLabel = (
  node: GraphNode,
  context: CanvasRenderingContext2D,
  x: number,
  y: number,
): void => {
  const label = nodeLabel(node);
  context.textAlign = "center";
  context.textBaseline = "middle";

  if (node.kind === "catalog") {
    context.fillStyle = CATALOG_TEXT_COLOR;
    context.fillText(label, x, y);
    return;
  }

  context.strokeStyle = NODE_TEXT_OUTLINE_COLOR;
  context.lineWidth = 10;
  context.lineJoin = "round";
  context.strokeText(label, x, y);
  context.fillStyle = NODE_TEXT_COLOR;
  context.fillText(label, x, y);
};

export const paintNode = (
  node: GraphNode,
  context: CanvasRenderingContext2D,
  metrics: GraphNodeMetrics,
  bounds: FrameBounds,
): void => {
  if (node.x === undefined || node.y === undefined) return;
  const { font, fontSize, width, height } = measureNode(node, context, metrics);
  const left = Math.round(node.x - width / 2);
  const top = Math.round(node.y - height / 2);
  if (!isBoxInFrame(bounds, left, top, left + width, top + height)) return;

  traceNodeShape(context, left, top, width, height);
  context.fillStyle = node.color;
  context.fill();
  strokeNodeBorder(node, context);
  if (isTextReadable(bounds, fontSize)) {
    context.font = font;
    fillNodeLabel(node, context, node.x, node.y);
  }
  context.lineWidth = 1;
};

export const paintNodePointerArea = (
  node: GraphNode,
  color: string,
  context: CanvasRenderingContext2D,
  metrics: GraphNodeMetrics,
): void => {
  if (node.x === undefined || node.y === undefined) return;
  const { width, height } = cachedNodeSize(node, metrics);
  context.fillStyle = color;
  context.fillRect(node.x - width / 2, node.y - height / 2, width, height);
};
