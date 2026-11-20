import { FileType } from "../../graphConstants";
import type { PartitionFiles } from "../partition/api/partitionFilesSchemas";
import type { SnapshotMap } from "../snapshots/api/snapshotQueries";
import type { GraphData } from "../table/api/graphSchemas";
import type { TableMetadata } from "../table/api/metadataSchemas";

const TABLE_METADATA_OPTION_ID = "table_metadata";
const ISSUES_OPTION_ID = "issues";
const SNAPSHOTS_OPTION_ID = "snapshots";
const DATA_FILES_OPTION_ID = "data_files";
const DATA_FILE_STATISTICS_OPTION_ID = "data_file_statistics";
const DATA_FILE_STATISTICS_FIELD = "readable_metrics";

const NODE_CONTEXT_OPTIONS = [
  {
    id: "metadata_files",
    label: "Metadata files",
    nodeTypes: [FileType.MAIN_METADATA, FileType.METADATA],
  },
  {
    id: SNAPSHOTS_OPTION_ID,
    label: "Snapshots",
    nodeTypes: [FileType.SNAPSHOT],
  },
  { id: "manifests", label: "Manifests", nodeTypes: [FileType.MANIFEST] },
  { id: DATA_FILES_OPTION_ID, label: "Data files", nodeTypes: [FileType.DATA] },
  {
    id: "delete_files",
    label: "Delete files",
    nodeTypes: [
      FileType.POSITION_DELETE,
      FileType.DELETION_VECTOR,
      FileType.EQUALITY_DELETE,
    ],
  },
  {
    id: "statistics_files",
    label: "Statistics files",
    nodeTypes: [FileType.TABLE_STATISTICS, FileType.PARTITION_STATISTICS],
  },
];

const GRAPH_DEFAULT_OPTION_IDS = [
  TABLE_METADATA_OPTION_ID,
  ISSUES_OPTION_ID,
  SNAPSHOTS_OPTION_ID,
];

const PARTITION_DEFAULT_OPTION_IDS = [
  TABLE_METADATA_OPTION_ID,
  ISSUES_OPTION_ID,
  "manifests",
];

const SNAPSHOT_SELECTION_DEFAULT_OPTION_IDS = [
  TABLE_METADATA_OPTION_ID,
  SNAPSHOTS_OPTION_ID,
];

const GRAPH_SUGGESTED_QUESTIONS = [
  "Summarize the state of this table.",
  "Explain what the latest snapshot changed.",
  "Do you see any problems or maintenance this table needs?",
];

const PARTITION_SUGGESTED_QUESTIONS = [
  "Summarize the files in this partition.",
  "Does this partition need compaction or delete cleanup?",
  "Which delete files apply to which data files?",
];

const SNAPSHOT_SELECTION_SUGGESTED_QUESTIONS = [
  "Summarize the state of this table.",
  "Summarize the recent snapshot history.",
  "Which snapshot range should I load to see the latest changes?",
];

export interface AssistantContextOption {
  id: string;
  label: string;
  itemCount: number | null;
}

export type AssistantPage = "table" | "snapshot_selection" | "partition";

export interface AssistantContextSource {
  page: AssistantPage;
  options: AssistantContextOption[];
  defaultOptionIds: readonly string[];
  suggestedQuestions: readonly string[];
  buildContext: (selectedOptionIds: readonly string[]) => string;
}

interface SnapshotMapPage {
  snapshots: SnapshotMap;
}

type GraphNode = GraphData["nodes"][number];

const serializeContext = (context: Record<string, unknown>): string =>
  Object.keys(context).length === 0 ? "" : JSON.stringify(context);

const countNodes = (graphData: GraphData, nodeTypes: string[]): number =>
  graphData.nodes.filter((node) => nodeTypes.includes(node.type)).length;

const hasDataFileStatistics = (node: GraphNode): boolean => {
  const statistics = node[DATA_FILE_STATISTICS_FIELD];
  return (
    node.type === FileType.DATA &&
    typeof statistics === "object" &&
    statistics !== null &&
    Object.keys(statistics).length > 0
  );
};

const withoutDataFileStatistics = (node: GraphNode): Record<string, unknown> =>
  Object.fromEntries(
    Object.entries(node).filter(([key]) => key !== DATA_FILE_STATISTICS_FIELD),
  );

const selectDataFileContext = (
  node: GraphNode,
  selectedOptionIds: readonly string[],
): Record<string, unknown> | null => {
  const isFileSelected = selectedOptionIds.includes(DATA_FILES_OPTION_ID);
  const isStatisticsSelected = selectedOptionIds.includes(
    DATA_FILE_STATISTICS_OPTION_ID,
  );
  if (isFileSelected && isStatisticsSelected) return node;
  if (isFileSelected) return withoutDataFileStatistics(node);
  if (isStatisticsSelected && hasDataFileStatistics(node)) {
    return {
      type: node.type,
      file_path: node.file_path,
      [DATA_FILE_STATISTICS_FIELD]: node[DATA_FILE_STATISTICS_FIELD],
    };
  }
  return null;
};

const listGraphContextOptions = (
  graphData: GraphData,
): AssistantContextOption[] => [
  { id: TABLE_METADATA_OPTION_ID, label: "Table metadata", itemCount: null },
  {
    id: ISSUES_OPTION_ID,
    label: "Issues",
    itemCount:
      Object.keys(graphData.errors).length +
      Object.keys(graphData.warnings).length,
  },
  ...NODE_CONTEXT_OPTIONS.flatMap(({ id, label, nodeTypes }) =>
    id === DATA_FILES_OPTION_ID
      ? [
          { id, label, itemCount: countNodes(graphData, nodeTypes) },
          {
            id: DATA_FILE_STATISTICS_OPTION_ID,
            label: "Data file statistics",
            itemCount: graphData.nodes.filter(hasDataFileStatistics).length,
          },
        ]
      : [{ id, label, itemCount: countNodes(graphData, nodeTypes) }],
  ).filter((option) => option.itemCount > 0),
];

const buildGraphContext = (
  graphData: GraphData,
  selectedOptionIds: readonly string[],
  baseContext: Record<string, unknown> = {},
): string => {
  const context: Record<string, unknown> = { ...baseContext };
  if (selectedOptionIds.includes(TABLE_METADATA_OPTION_ID)) {
    context.metadata = graphData.metadata;
  }
  if (selectedOptionIds.includes(ISSUES_OPTION_ID)) {
    context.errors = graphData.errors;
    context.warnings = graphData.warnings;
  }
  const selectedNodeTypes = NODE_CONTEXT_OPTIONS.filter(
    (option) =>
      option.id !== DATA_FILES_OPTION_ID &&
      selectedOptionIds.includes(option.id),
  ).flatMap((option) => option.nodeTypes);
  const nodes = graphData.nodes.flatMap((node) => {
    if (node.type === FileType.DATA) {
      const dataFileContext = selectDataFileContext(node, selectedOptionIds);
      return dataFileContext === null ? [] : [dataFileContext];
    }
    return selectedNodeTypes.includes(node.type) ? [node] : [];
  });
  if (nodes.length > 0) {
    context.nodes = nodes;
  }
  return serializeContext(context);
};

export const createGraphContextSource = (
  graphData: GraphData,
): AssistantContextSource => ({
  page: "table",
  options: listGraphContextOptions(graphData),
  defaultOptionIds: GRAPH_DEFAULT_OPTION_IDS,
  suggestedQuestions: GRAPH_SUGGESTED_QUESTIONS,
  buildContext: (selectedOptionIds) =>
    buildGraphContext(graphData, selectedOptionIds),
});

export const createPartitionContextSource = (
  partitionFiles: PartitionFiles,
  metadata: TableMetadata,
): AssistantContextSource => {
  const graphData: GraphData = {
    nodes: partitionFiles.nodes,
    metadata,
    errors: partitionFiles.errors,
    warnings: partitionFiles.warnings,
  };
  return {
    page: "partition",
    options: listGraphContextOptions(graphData),
    defaultOptionIds: PARTITION_DEFAULT_OPTION_IDS,
    suggestedQuestions: PARTITION_SUGGESTED_QUESTIONS,
    buildContext: (selectedOptionIds) =>
      buildGraphContext(graphData, selectedOptionIds, {
        partition: partitionFiles.summary,
      }),
  };
};

const countSnapshots = (snapshotPages: readonly SnapshotMapPage[]): number =>
  snapshotPages.reduce(
    (snapshotCount, page) => snapshotCount + Object.keys(page.snapshots).length,
    0,
  );

export const createSnapshotSelectionContextSource = (
  metadata: TableMetadata | undefined,
  snapshotPages: readonly SnapshotMapPage[] | undefined,
): AssistantContextSource => ({
  page: "snapshot_selection",
  options: [
    ...(metadata === undefined
      ? []
      : [
          {
            id: TABLE_METADATA_OPTION_ID,
            label: "Table metadata",
            itemCount: null,
          },
        ]),
    ...(snapshotPages === undefined
      ? []
      : [
          {
            id: SNAPSHOTS_OPTION_ID,
            label: "Snapshots",
            itemCount: countSnapshots(snapshotPages),
          },
        ]),
  ],
  defaultOptionIds: SNAPSHOT_SELECTION_DEFAULT_OPTION_IDS,
  suggestedQuestions: SNAPSHOT_SELECTION_SUGGESTED_QUESTIONS,
  buildContext: (selectedOptionIds) => {
    const context: Record<string, unknown> = {};
    if (
      metadata !== undefined &&
      selectedOptionIds.includes(TABLE_METADATA_OPTION_ID)
    ) {
      context.metadata = metadata;
    }
    if (
      snapshotPages !== undefined &&
      selectedOptionIds.includes(SNAPSHOTS_OPTION_ID)
    ) {
      context.snapshots = Object.fromEntries(
        snapshotPages.flatMap((page) => Object.entries(page.snapshots)),
      );
    }
    return serializeContext(context);
  },
});
