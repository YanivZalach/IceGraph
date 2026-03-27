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
  [FileType.MAIN_METADATA]: { rgb: [250, 112, 181], level: -1 },
  [FileType.METADATA]: { rgb: [191, 90, 242], level: -1 },
  [FileType.SNAPSHOT]: { rgb: [58, 190, 249], level: 0 },
  [FileType.MANIFEST]: { rgb: [243, 156, 18], level: 1 },
  [FileType.DATA]: { rgb: [46, 204, 113], level: 2 },
  [FileType.POSITION_DELETE]: { rgb: [231, 76, 60], level: 2 },
  [FileType.EQUALITY_DELETE]: { rgb: [231, 76, 60], level: 2 },
}
export const UI_SECTION_NEWLINE = '\x00'
export const UI_NEWLINE = '\n'

export const VISUALIZATION_OPTIONS = {
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
    navigationButtons: true,
    multiselect: true,
    tooltipDelay: 100,
    hideEdgesOnDrag: true,
  },
}
