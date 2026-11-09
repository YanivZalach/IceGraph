import {
  CATALOG_NODE_ID,
  CURRENT_METADATA_NODE_ID,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  DELETION_VECTOR_CONNECTION_LABEL,
  FileType,
  GRAPH_SETTINGS,
  NODE_STYLE_MAP,
  fileTypeLabel,
} from "../../../graphConstants";
import type { TableMetadata } from "../../table/api/metadataSchemas";
import type {
  GraphFileEdge,
  GraphFileNode,
  GraphNodeDetails,
  GraphViewData,
} from "../graphViewSchemas";
import { buildLineageAdjacency, type LineageAdjacency } from "./lineage";

export type GraphNodeKind = "file" | "catalog" | "current_metadata";
export type GraphLinkBend = "none" | "right";
export type CurrentMetadataStatus = "loading" | "unavailable" | "resolved";

interface GraphNodeContent {
  id: string;
  kind: GraphNodeKind;
  type: string;
  label: string;
  details: GraphNodeDetails;
  color: string;
  level: number;
  currentMetadataStatus?: CurrentMetadataStatus;
}

export interface GraphNode extends GraphNodeContent {
  layoutX: number;
  layoutY: number;
  x?: number;
  y?: number;
  vx?: number;
  vy?: number;
  fx?: number;
  fy?: number;
}

export interface GraphLink {
  source: string;
  target: string;
  color: string;
  label: string;
  curvature: number;
  bend: GraphLinkBend;
  labelPosition: number;
}

export interface FileGraph {
  nodes: GraphNode[];
  links: GraphLink[];
  catalog: GraphNode | null;
  mainMetadata: GraphNode | null;
}

export interface GraphModel {
  nodes: GraphNode[];
  nodesById: ReadonlyMap<string, GraphNode>;
  links: GraphLink[];
  lineage: LineageAdjacency;
}

const DEFAULT_LINK_COLOR = "#999";
const LINK_CURVATURE = 0.1;
const DELETED_CONNECTION_LABEL = "deleted";

const fileNameFromPath = (filePath: string): string =>
  filePath.split("/").pop() ?? filePath;

const styleColor = (type: string): string => {
  const rgb = NODE_STYLE_MAP[type]?.rgb ?? [100, 100, 100];
  return `rgb(${rgb.join(",")})`;
};

const toFileLink = (edge: GraphFileEdge): GraphLink => {
  const isDeleted = edge.color === DELETED_DATA_FILE_CONNECTION_COLOR;
  const isDeletionVector = edge.deletes_from_data_file === true;
  const label = isDeleted
    ? DELETED_CONNECTION_LABEL
    : isDeletionVector
      ? DELETION_VECTOR_CONNECTION_LABEL
      : (edge.branch_names ?? "");

  return {
    source: edge.from,
    target: edge.to,
    color: edge.color ?? DEFAULT_LINK_COLOR,
    label,
    curvature:
      edge.curvature ?? (label === "" || isDeleted ? 0 : LINK_CURVATURE),
    bend: isDeletionVector ? "right" : "none",
    labelPosition: isDeleted || isDeletionVector ? 0.5 : 0.25,
  };
};

const linkBetween = (source: string, target: string): GraphLink => ({
  source,
  target,
  color: DEFAULT_LINK_COLOR,
  label: "",
  curvature: 0,
  bend: "none",
  labelPosition: 0.25,
});

const buildCatalogNode = (metadata: TableMetadata): GraphNodeContent => ({
  id: CATALOG_NODE_ID,
  kind: "catalog",
  type: FileType.CATALOG,
  label: [fileTypeLabel(FileType.CATALOG), metadata["table-name"]]
    .filter(Boolean)
    .join(" · "),
  details: {
    type: FileType.CATALOG,
    "table-name": metadata["table-name"],
    "table-uuid": metadata["table-uuid"],
    location: metadata.location,
    "format-version": metadata["format-version"],
  },
  color: styleColor(FileType.CATALOG),
  level: NODE_STYLE_MAP[FileType.CATALOG]?.level ?? -1,
});

const currentMetadataNode = (
  currentMetadataStatus: CurrentMetadataStatus,
  label: string,
  details: GraphNodeDetails,
): GraphNodeContent => ({
  id: CURRENT_METADATA_NODE_ID,
  kind: "current_metadata",
  currentMetadataStatus,
  type: FileType.CURRENT_METADATA,
  label,
  details: { type: FileType.CURRENT_METADATA, ...details },
  color: styleColor(FileType.CURRENT_METADATA),
  level: NODE_STYLE_MAP[FileType.CURRENT_METADATA]?.level ?? -1,
});

const buildCurrentMetadataNode = (
  mainMetadata: GraphNode,
  currentMetadataFile: string | undefined,
  currentMetadataError: Error | null,
): GraphNodeContent | null => {
  if (currentMetadataFile === mainMetadata.details.file_path) return null;
  if (currentMetadataFile !== undefined) {
    return currentMetadataNode(
      "resolved",
      fileNameFromPath(currentMetadataFile),
      { file_path: currentMetadataFile, virtual_read: true },
    );
  }
  return currentMetadataError === null
    ? currentMetadataNode("loading", "Current metadata · loading…", {})
    : currentMetadataNode("unavailable", "Current metadata · unavailable", {
        errors: [currentMetadataError.message],
      });
};

const toFileNodeContent = (node: GraphFileNode): GraphNodeContent => ({
  ...node,
  kind: "file",
});

const layoutByLevel = (nodes: GraphNodeContent[]): GraphNode[] => {
  const { levelSeparation, nodeSpacing } = GRAPH_SETTINGS;
  const levelSizes = new Map<number, number>();
  for (const node of nodes) {
    levelSizes.set(node.level, (levelSizes.get(node.level) ?? 0) + 1);
  }

  const levelPositions = new Map<number, number>();
  return nodes.map((node) => {
    const position = levelPositions.get(node.level) ?? 0;
    levelPositions.set(node.level, position + 1);
    const levelSize = levelSizes.get(node.level) ?? 1;
    const layoutX = node.level * levelSeparation;
    const layoutY =
      position * nodeSpacing - ((levelSize - 1) * nodeSpacing) / 2;
    return { ...node, layoutX, layoutY, fx: layoutX, fy: layoutY };
  });
};

const placeBelowCatalog = (
  node: GraphNodeContent,
  catalog: GraphNode,
): GraphNode => {
  const layoutX = catalog.layoutX;
  const layoutY = catalog.layoutY + GRAPH_SETTINGS.nodeSpacing / 2;
  return { ...node, layoutX, layoutY, fx: layoutX, fy: layoutY };
};

const entryLinks = (
  mainMetadata: GraphNode,
  currentMetadata: GraphNodeContent | null,
): GraphLink[] => {
  if (currentMetadata === null) {
    return [linkBetween(CATALOG_NODE_ID, mainMetadata.id)];
  }
  const links = [linkBetween(CATALOG_NODE_ID, CURRENT_METADATA_NODE_ID)];
  if (currentMetadata.currentMetadataStatus === "resolved") {
    links.push(linkBetween(CURRENT_METADATA_NODE_ID, mainMetadata.id));
  }
  return links;
};

export const buildFileGraph = (graph: GraphViewData): FileGraph => {
  const hasMainMetadata = graph.nodes.some(
    (node) => node.type === FileType.MAIN_METADATA,
  );
  const nodes = layoutByLevel([
    ...(hasMainMetadata ? [buildCatalogNode(graph.metadata)] : []),
    ...graph.nodes.map(toFileNodeContent),
  ]);
  return {
    nodes,
    links: graph.edges.map(toFileLink),
    catalog: nodes.find((node) => node.kind === "catalog") ?? null,
    mainMetadata:
      nodes.find((node) => node.type === FileType.MAIN_METADATA) ?? null,
  };
};

export const buildGraphModel = (
  fileGraph: FileGraph,
  currentMetadataFile: string | undefined,
  currentMetadataError: Error | null,
): GraphModel => {
  const { catalog, mainMetadata } = fileGraph;
  if (catalog === null || mainMetadata === null) {
    return {
      nodes: fileGraph.nodes,
      nodesById: new Map(fileGraph.nodes.map((node) => [node.id, node])),
      links: fileGraph.links,
      lineage: buildLineageAdjacency(fileGraph.links),
    };
  }

  const currentMetadata = buildCurrentMetadataNode(
    mainMetadata,
    currentMetadataFile,
    currentMetadataError,
  );
  const nodes =
    currentMetadata === null
      ? fileGraph.nodes
      : [...fileGraph.nodes, placeBelowCatalog(currentMetadata, catalog)];
  const links = [
    ...fileGraph.links,
    ...entryLinks(mainMetadata, currentMetadata),
  ];
  return {
    nodes,
    nodesById: new Map(nodes.map((node) => [node.id, node])),
    links,
    lineage: buildLineageAdjacency(links),
  };
};
