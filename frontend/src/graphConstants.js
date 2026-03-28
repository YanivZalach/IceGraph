export const DELETED_DATA_FILE_CONNECTION_COLOR = '#FF0000'
export const BRANCH_CONNECTION_COLOR = '#0000FF'

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
export const UI_SECTION_NEWLINE = '\x00'
export const UI_NEWLINE = '\n'

export const VISUALIZATION_OPTIONS = {
  nodes: {
    font: {
      color: '#ffffff',
      size: 16,
      bold: true,
      strokeWidth: 1,
      strokeColor: 'rgba(0,0,0,0.6)',
    },
  },
  layout: {
    hierarchical: {
      enabled: true,
      direction: 'LR',
      nodeSpacing: 150,
      levelSeparation: 800,
      sortMethod: 'directed',
      blockShifting: true,
      edgeMinimization: true,
      parentCentralization: true,
    },
    improvedLayout: true,
  },
  edges: {
    color: '#999',
    smooth: {
      enabled: true,
      type: 'cubicBezier',
      forceDirection: 'horizontal',
      roundness: 0.5,
    },
  },
  physics: {
    enabled: false,
  },
  interaction: {
    hover: true,
    navigationButtons: false,
    multiselect: true,
    tooltipDelay: 100,
    hideEdgesOnDrag: true,
  },
}
