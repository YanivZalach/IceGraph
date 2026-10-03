export const DELETED_DATA_FILE_CONNECTION_COLOR = "#FF0000";
export const BRANCH_CONNECTION_COLOR = "rgba(56, 189, 248, 0.5)";
export const STATISTICS_CONNECTION_CURVATURE = 0.3;
export const CATALOG_NODE_ID = "catalog";
export const CATALOG_NODE_SCALE = 1.5;
export const MAIN_BRANCH_NAME = "main";

export const FileType = {
  MAIN_METADATA: "main_metadata",
  METADATA: "metadata",
  SNAPSHOT: "snapshot",
  MANIFEST: "manifest",
  DATA: "data",
  POSITION_DELETE: "position_delete",
  EQUALITY_DELETE: "equality_delete",
  TABLE_STATISTICS: "table_statistics",
  PARTITION_STATISTICS: "partition_statistics",
  CATALOG: "catalog",
};

export const STATISTICS_CONNECTION_COLORS = {
  [FileType.TABLE_STATISTICS]: "rgba(245, 158, 11, 0.8)",
  [FileType.PARTITION_STATISTICS]: "rgba(203, 213, 225, 0.8)",
};

export const NODE_STYLE_MAP = {
  [FileType.MAIN_METADATA]: { rgb: [195, 60, 130], level: -1 },
  [FileType.METADATA]: { rgb: [100, 55, 210], level: -1 },
  [FileType.SNAPSHOT]: { rgb: [25, 100, 185], level: 0 },
  [FileType.MANIFEST]: { rgb: [25, 145, 185], level: 1 },
  [FileType.DATA]: { rgb: [25, 150, 115], level: 2 },
  [FileType.POSITION_DELETE]: { rgb: [230, 145, 30], level: 2 },
  [FileType.EQUALITY_DELETE]: { rgb: [230, 145, 30], level: 2 },
  [FileType.TABLE_STATISTICS]: { rgb: [217, 119, 6], level: -2 },
  [FileType.PARTITION_STATISTICS]: { rgb: [148, 163, 184], level: -2 },
  [FileType.CATALOG]: { rgb: [226, 232, 240], level: -1 },
};

export const ERROR_NODE_RGB = [185, 35, 60];
const FILE_TYPE_LABELS = {
  [FileType.MAIN_METADATA]: "Main Metadata",
  [FileType.METADATA]: "Metadata",
  [FileType.SNAPSHOT]: "Snapshot",
  [FileType.MANIFEST]: "Manifest",
  [FileType.DATA]: "Data File",
  [FileType.POSITION_DELETE]: "Position Delete",
  [FileType.EQUALITY_DELETE]: "Equality Delete",
  [FileType.TABLE_STATISTICS]: "Table Statistics",
  [FileType.PARTITION_STATISTICS]: "Partition Statistics",
  [FileType.CATALOG]: "Catalog",
};

export function fileTypeLabel(type) {
  if (!type) return "Details";
  return FILE_TYPE_LABELS[type] ?? String(type).replace(/_/g, " ");
}
export const GRAPH_SETTINGS = {
  levelSeparation: 4200,
  nodeSpacing: 700,
};
