import { CATALOG_NODE_SCALE } from "../../../graphConstants";
import {
  GRAPH_NODE_FONT_REM,
  GRAPH_NODE_PADDING_X_REM,
  GRAPH_NODE_PADDING_Y_REM,
  remToPx,
} from "../../../layoutConstants";
import type { GraphNode } from "../model/graphModel";

const LINK_FONT_REM = 3.75;

export interface GraphNodeMetrics {
  fontSize: number;
  paddingX: number;
  paddingY: number;
  linkFontSize: number;
}

export interface NodeSize {
  fontSize: number;
  font: string;
  width: number;
  height: number;
}

const nodeSizeCache = new WeakMap<GraphNode, NodeSize>();

export const getGraphNodeMetrics = (): GraphNodeMetrics => ({
  fontSize: remToPx(GRAPH_NODE_FONT_REM),
  paddingX: remToPx(GRAPH_NODE_PADDING_X_REM),
  paddingY: remToPx(GRAPH_NODE_PADDING_Y_REM),
  linkFontSize: remToPx(LINK_FONT_REM),
});

export const nodeLabel = (node: GraphNode): string => node.label || node.id;

export const measureNode = (
  node: GraphNode,
  context: CanvasRenderingContext2D,
  metrics: GraphNodeMetrics,
): NodeSize => {
  const scale = node.kind === "catalog" ? CATALOG_NODE_SCALE : 1;
  const fontSize = metrics.fontSize * scale;
  const cached = nodeSizeCache.get(node);
  if (cached !== undefined && cached.fontSize === fontSize) return cached;

  const fontWeight = node.kind === "catalog" ? 700 : 500;
  const font = `${String(fontWeight)} ${String(fontSize)}px "system-ui"`;
  context.font = font;
  const size = {
    fontSize,
    font,
    width:
      context.measureText(nodeLabel(node)).width + metrics.paddingX * 2 * scale,
    height: fontSize + metrics.paddingY * 2 * scale,
  };
  nodeSizeCache.set(node, size);
  return size;
};

export const cachedNodeSize = (
  node: GraphNode,
  metrics: GraphNodeMetrics,
): Pick<NodeSize, "width" | "height"> =>
  nodeSizeCache.get(node) ?? {
    width: remToPx(2.5),
    height: metrics.fontSize + metrics.paddingY * 2,
  };
