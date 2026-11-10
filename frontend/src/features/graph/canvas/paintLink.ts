import type { GraphLink, GraphNode } from "../model/graphModel";
import { isBoxInFrame, isTextReadable, type FrameBounds } from "./frameBounds";
import { measureNode, type GraphNodeMetrics } from "./nodeMetrics";

const LINK_LABEL_COLOR = "#e3f8f5ff";
const ARROW_LENGTH_PX = 8;
const DASHED_LINK_PATTERN = [24, 16];

interface Point {
  x: number;
  y: number;
}

interface LinkGeometry {
  start: Point;
  control: Point;
  end: Point;
}

interface PositionedEnd extends Point {
  width: number;
  height: number;
}

interface LinkPaintContext {
  context: CanvasRenderingContext2D;
  metrics: GraphNodeMetrics;
  bounds: FrameBounds;
}

interface VisibleLink {
  link: GraphLink;
  geometry: LinkGeometry;
}

const straightGeometry = (
  source: PositionedEnd,
  target: PositionedEnd,
  curvature: number,
): LinkGeometry | null => {
  const isVertical =
    Math.abs(target.x - source.x) < (source.width + target.width) / 2;
  const direction = Math.sign(
    isVertical ? target.y - source.y : target.x - source.x,
  );
  const start = {
    x: source.x + (isVertical ? 0 : (direction * source.width) / 2),
    y: source.y + (isVertical ? (direction * source.height) / 2 : 0),
  };
  const end = {
    x: target.x - (isVertical ? 0 : (direction * target.width) / 2),
    y: target.y - (isVertical ? (direction * target.height) / 2 : 0),
  };
  if (direction * (isVertical ? end.y - start.y : end.x - start.x) <= 0) {
    return null;
  }

  const control = {
    x: Math.max(
      Math.min(start.x, end.x),
      Math.min(
        Math.max(start.x, end.x),
        (start.x + end.x) / 2 + (end.y - start.y) * curvature,
      ),
    ),
    y: isVertical
      ? (start.y + end.y) / 2
      : (start.y + end.y) / 2 - (end.x - start.x) * curvature,
  };
  return { start, control, end };
};

const rightBendGeometry = (
  source: PositionedEnd,
  target: PositionedEnd,
): LinkGeometry => {
  const start = { x: source.x + source.width / 2, y: source.y };
  const end = { x: target.x + target.width / 2, y: target.y };
  return {
    start,
    end,
    control: {
      x: Math.max(start.x, end.x) + Math.abs(end.y - start.y),
      y: (start.y + end.y) / 2,
    },
  };
};

const quadraticPoint = (geometry: LinkGeometry, position: number): Point => {
  const remaining = 1 - position;
  const { start, control, end } = geometry;
  return {
    x:
      remaining ** 2 * start.x +
      2 * remaining * position * control.x +
      position ** 2 * end.x,
    y:
      remaining ** 2 * start.y +
      2 * remaining * position * control.y +
      position ** 2 * end.y,
  };
};

const traceArrow = (
  context: CanvasRenderingContext2D,
  { start, control, end }: LinkGeometry,
  globalScale: number,
): void => {
  const angle = Math.atan2(end.y - control.y, end.x - control.x);
  const length = Math.min(
    ARROW_LENGTH_PX / globalScale,
    Math.hypot(end.x - start.x, end.y - start.y) / 3,
  );
  const baseX = end.x - Math.cos(angle) * length;
  const baseY = end.y - Math.sin(angle) * length;
  context.moveTo(end.x, end.y);
  context.lineTo(
    baseX + (Math.sin(angle) * length) / 2,
    baseY - (Math.cos(angle) * length) / 2,
  );
  context.lineTo(
    baseX - (Math.sin(angle) * length) / 2,
    baseY + (Math.cos(angle) * length) / 2,
  );
  context.closePath();
};

const positionedEnd = (
  node: GraphNode,
  { context, metrics }: LinkPaintContext,
): PositionedEnd | null =>
  node.x === undefined || node.y === undefined
    ? null
    : { x: node.x, y: node.y, ...measureNode(node, context, metrics) };

const linkGeometry = (
  link: GraphLink,
  source: GraphNode,
  target: GraphNode,
  paint: LinkPaintContext,
): LinkGeometry | null => {
  const sourceEnd = positionedEnd(source, paint);
  const targetEnd = positionedEnd(target, paint);
  if (sourceEnd === null || targetEnd === null) return null;
  const straight = straightGeometry(sourceEnd, targetEnd, link.curvature);
  if (straight === null) return null;
  return link.bend === "right"
    ? rightBendGeometry(sourceEnd, targetEnd)
    : straight;
};

const isGeometryInFrame = (
  { start, control, end }: LinkGeometry,
  bounds: FrameBounds,
): boolean =>
  isBoxInFrame(
    bounds,
    Math.min(start.x, control.x, end.x),
    Math.min(start.y, control.y, end.y),
    Math.max(start.x, control.x, end.x),
    Math.max(start.y, control.y, end.y),
  );

const groupVisibleLinksByColor = (
  links: GraphLink[],
  nodesById: ReadonlyMap<string, GraphNode>,
  isNodeVisible: (nodeId: string) => boolean,
  paint: LinkPaintContext,
): Map<string, VisibleLink[]> => {
  const linksByColor = new Map<string, VisibleLink[]>();
  for (const link of links) {
    if (!isNodeVisible(link.source) || !isNodeVisible(link.target)) continue;
    const source = nodesById.get(link.source);
    const target = nodesById.get(link.target);
    if (source === undefined || target === undefined) continue;
    const geometry = linkGeometry(link, source, target, paint);
    if (geometry === null || !isGeometryInFrame(geometry, paint.bounds)) {
      continue;
    }
    const colorLinks = linksByColor.get(link.color);
    if (colorLinks === undefined)
      linksByColor.set(link.color, [{ link, geometry }]);
    else colorLinks.push({ link, geometry });
  }
  return linksByColor;
};

const strokeLinkLines = (
  context: CanvasRenderingContext2D,
  visibleLinks: VisibleLink[],
  dashed: boolean,
): void => {
  context.setLineDash(dashed ? DASHED_LINK_PATTERN : []);
  context.beginPath();
  for (const { link, geometry } of visibleLinks) {
    if (link.dashed !== dashed) continue;
    context.moveTo(geometry.start.x, geometry.start.y);
    context.quadraticCurveTo(
      geometry.control.x,
      geometry.control.y,
      geometry.end.x,
      geometry.end.y,
    );
  }
  context.stroke();
  context.setLineDash([]);
};

const strokeColorGroup = (
  context: CanvasRenderingContext2D,
  color: string,
  visibleLinks: VisibleLink[],
  globalScale: number,
): void => {
  context.strokeStyle = color;
  context.fillStyle = color;
  strokeLinkLines(context, visibleLinks, false);
  strokeLinkLines(context, visibleLinks, true);
  context.beginPath();
  for (const { geometry } of visibleLinks) {
    traceArrow(context, geometry, globalScale);
  }
  context.fill();
};

const fillLinkLabels = (
  context: CanvasRenderingContext2D,
  visibleLinks: VisibleLink[],
): void => {
  for (const { link, geometry } of visibleLinks) {
    if (link.label === "") continue;
    const labelPoint = quadraticPoint(geometry, link.labelPosition);
    context.fillText(link.label, labelPoint.x, labelPoint.y);
  }
};

export const paintLinks = (
  links: GraphLink[],
  nodesById: ReadonlyMap<string, GraphNode>,
  isNodeVisible: (nodeId: string) => boolean,
  paint: LinkPaintContext,
): void => {
  const { context, metrics, bounds } = paint;
  const linksByColor = groupVisibleLinksByColor(
    links,
    nodesById,
    isNodeVisible,
    paint,
  );

  context.save();
  context.lineWidth = 1 / bounds.globalScale;
  for (const [color, visibleLinks] of linksByColor) {
    strokeColorGroup(context, color, visibleLinks, bounds.globalScale);
  }
  if (isTextReadable(bounds, metrics.linkFontSize)) {
    context.font = `500 ${String(metrics.linkFontSize)}px "system-ui"`;
    context.textAlign = "center";
    context.textBaseline = "middle";
    context.fillStyle = LINK_LABEL_COLOR;
    for (const visibleLinks of linksByColor.values()) {
      fillLinkLabels(context, visibleLinks);
    }
  }
  context.restore();
};
