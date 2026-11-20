import { formatLocaleDateTime, parseUtcDate } from "./dateUtils";
import {
  BRANCH_CONNECTION_COLOR,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  DELETION_VECTOR_CONNECTION_COLOR,
  ERROR_NODE_RGB,
  FileType,
  MAIN_BRANCH_NAME,
  NODE_STYLE_MAP,
  STATISTICS_CONNECTION_COLORS,
  STATISTICS_CONNECTION_CURVATURE,
} from "../graphConstants";

const localizeNodeTimestamps = (details) => {
  if (!details) return {};

  const result = { ...details };

  for (const key of Object.keys(result)) {
    if (!key.includes("timestamp")) continue;

    try {
      const dateObj = parseUtcDate(result[key]);
      if (dateObj) {
        result[key] = formatLocaleDateTime(dateObj);
      }
    } catch (e) {
      console.error(
        "Failed to parse timestamp key:",
        key,
        "value:",
        result[key],
        "error:",
        e,
      );
    }
  }

  return result;
};

const METADATA_COLOR_SHIFT_SPREAD = 1.5;

const fileNameFromPath = (filePath) =>
  String(filePath ?? "")
    .split("/")
    .pop();

const buildMetadataColorShifts = (nodeDetails) => {
  const metadataDetails = nodeDetails
    .filter(
      (d) => d.type === FileType.MAIN_METADATA || d.type === FileType.METADATA,
    )
    .map((d) => ({
      filePath: d.file_path,
      time: parseUtcDate(d.timestamp)?.getTime() ?? 0,
    }))
    .sort((a, b) => b.time - a.time);

  return new Map(
    metadataDetails.map((d, index) => [
      d.filePath,
      1 - index / (METADATA_COLOR_SHIFT_SPREAD * metadataDetails.length),
    ]),
  );
};

const buildEdgesFromNodes = (nodes) => {
  const nodeIds = new Set(nodes.map((n) => n.id));
  const snapshotPathById = {};
  nodes.forEach((n) => {
    if (n.type === FileType.SNAPSHOT && n.details?.snapshot_id != null) {
      snapshotPathById[String(n.details.snapshot_id)] = n.id;
    }
  });
  const statisticsTypeById = new Map(
    nodes
      .filter((n) => n.type in STATISTICS_CONNECTION_COLORS)
      .map((n) => [n.id, n.type]),
  );

  const edges = [];
  nodes.forEach((node) => {
    const details = node.details || {};

    if (
      node.type === FileType.MAIN_METADATA ||
      node.type === FileType.METADATA
    ) {
      const mainPath =
        details.snapshot_id != null
          ? snapshotPathById[String(details.snapshot_id)]
          : null;
      if (mainPath && nodeIds.has(mainPath)) {
        edges.push({ from: node.id, to: mainPath });
      }

      const branchNamesBySnapId = {};
      Object.entries(details.refs || {}).forEach(([name, attrs]) => {
        if (attrs?.type === "branch" && name !== MAIN_BRANCH_NAME) {
          const snapId = String(attrs["snapshot-id"]);
          if (!branchNamesBySnapId[snapId]) branchNamesBySnapId[snapId] = [];
          branchNamesBySnapId[snapId].push(name);
        }
      });
      Object.entries(branchNamesBySnapId).forEach(([snapId, names]) => {
        const branchPath = snapshotPathById[snapId];
        if (branchPath && nodeIds.has(branchPath)) {
          edges.push({
            from: node.id,
            to: branchPath,
            branch_names: names.join(", "),
          });
        }
      });

      (details.child_files || []).forEach((childPath) => {
        const statisticsType = statisticsTypeById.get(childPath);
        if (statisticsType) {
          edges.push({
            from: node.id,
            to: childPath,
            statistics_type: statisticsType,
          });
        }
      });
      return;
    }

    if (
      node.type === FileType.DELETION_VECTOR &&
      nodeIds.has(details.referenced_data_file)
    ) {
      edges.push({
        from: node.id,
        to: details.referenced_data_file,
        deletes_from_data_file: true,
      });
    }

    const deleted = new Set(details.deleted_child_files || []);
    const childFiles = details.child_files || [];
    childFiles.forEach((childPath) => {
      if (!nodeIds.has(childPath)) return;
      const edge = { from: node.id, to: childPath };
      if (node.type === FileType.MANIFEST && deleted.has(childPath))
        edge.is_deleted = true;
      edges.push(edge);
    });
  });
  return edges;
};

export const buildGraphData = (data) => {
  const nodeDetails = data.nodes || [];
  const colorShiftByFilePath = buildMetadataColorShifts(nodeDetails);

  const styledNodes = nodeDetails.map((details) => {
    const style = NODE_STYLE_MAP[details.type] || {
      rgb: [100, 100, 100],
      level: 0,
    };
    const hasFileErrors = details.errors?.length > 0;
    const [r, g, b] = hasFileErrors ? ERROR_NODE_RGB : style.rgb;
    const colorShift = hasFileErrors
      ? 1
      : (colorShiftByFilePath.get(details.file_path) ?? 1);

    const nodeId = details.node_id;

    return {
      id: nodeId,
      label: fileNameFromPath(nodeId),
      type: details.type,
      details: localizeNodeTimestamps(details),
      shape: "box",
      color: `rgba(${r},${g},${b},${colorShift})`,
      level: style.level,
    };
  });
  const styledEdges = buildEdgesFromNodes(styledNodes).map((edge) => {
    const newEdge = { ...edge };
    if (edge.is_deleted) {
      newEdge.color = DELETED_DATA_FILE_CONNECTION_COLOR;
      newEdge.title = "deleted";
    } else if (edge.branch_names) {
      newEdge.dashes = [15, 20, 5, 20];
      newEdge.color = BRANCH_CONNECTION_COLOR;
      newEdge.title = edge.branch_names;
    } else if (edge.deletes_from_data_file) {
      newEdge.color = DELETION_VECTOR_CONNECTION_COLOR;
    } else if (edge.statistics_type) {
      newEdge.color = STATISTICS_CONNECTION_COLORS[edge.statistics_type];
      newEdge.curvature = STATISTICS_CONNECTION_CURVATURE;
    }
    return newEdge;
  });
  return {
    nodes: styledNodes,
    edges: styledEdges,
    metadata: data.metadata,
    errors: data.errors || {},
  };
};
