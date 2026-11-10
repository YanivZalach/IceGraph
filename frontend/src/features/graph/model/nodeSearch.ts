import { fileTypeLabel } from "../../../graphConstants";
import type { GraphNode } from "./graphModel";

export const MAX_LISTED_SEARCH_RESULTS = 50;

const searchableTextCache = new WeakMap<GraphNode, string>();

const searchableText = (node: GraphNode): string => {
  const cached = searchableTextCache.get(node);
  if (cached !== undefined) return cached;
  const text = [
    node.label,
    node.id,
    node.details.file_path ?? "",
    fileTypeLabel(node.type),
  ]
    .join("\n")
    .toLowerCase();
  searchableTextCache.set(node, text);
  return text;
};

export const normalizeSearchQuery = (query: string): string =>
  query.trim().toLowerCase();

export const findMatchingNodes = (
  nodes: GraphNode[],
  query: string,
  isVisible: (nodeId: string) => boolean,
): GraphNode[] => {
  const needle = normalizeSearchQuery(query);
  if (needle === "") return [];
  return nodes.filter(
    (node) => isVisible(node.id) && searchableText(node).includes(needle),
  );
};
