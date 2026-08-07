export const DELETED_DATA_FILE_CONNECTION_COLOR = '#FF0000'
export const BRANCH_CONNECTION_COLOR = 'rgba(56, 189, 248, 0.5)';
export const MAIN_BRANCH_NAME = 'main'

export const FileType = {
  MAIN_METADATA: 'main_metadata',
  METADATA: 'metadata',
  SNAPSHOT: 'snapshot',
  MANIFEST: 'manifest',
  DATA: 'data',
  POSITION_DELETE: 'position_delete',
  EQUALITY_DELETE: 'equality_delete',
}

export const NODE_STYLE_MAP = {
  [FileType.MAIN_METADATA]: { rgb: [195, 60, 130], level: -1 },
  [FileType.METADATA]: { rgb: [100, 55, 210], level: -1 },
  [FileType.SNAPSHOT]: { rgb: [25, 100, 185], level: 0 },
  [FileType.MANIFEST]: { rgb: [25, 145, 185], level: 1 },
  [FileType.DATA]: { rgb: [25, 150, 115], level: 2 },
  [FileType.POSITION_DELETE]: { rgb: [185, 35, 60], level: 2 },
  [FileType.EQUALITY_DELETE]: { rgb: [185, 35, 60], level: 2 },
}
const FILE_TYPE_LABELS = {
  [FileType.MAIN_METADATA]: 'Main Metadata',
  [FileType.METADATA]: 'Metadata',
  [FileType.SNAPSHOT]: 'Snapshot',
  [FileType.MANIFEST]: 'Manifest',
  [FileType.DATA]: 'Data File',
  [FileType.POSITION_DELETE]: 'Position Delete',
  [FileType.EQUALITY_DELETE]: 'Equality Delete',
}

export function fileTypeLabel(type) {
  if (!type) return 'Details'
  return FILE_TYPE_LABELS[type] ?? String(type).replace(/_/g, ' ')
}
export const GRAPH_SETTINGS = {
  levelSeparation: 3000,
  nodeSpacing: 700,
}
