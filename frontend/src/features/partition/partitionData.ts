import { createContext, useContext } from "react";
import { z } from "zod";
import {
  DELETION_VECTOR_CONNECTION_COLOR,
  FileType,
} from "../../graphConstants";
import { buildGraphData } from "../../utils/tableGraphData";
import type { TableMetadata } from "../table/api/metadataSchemas";
import type {
  PartitionFiles,
  PartitionFilesSummary,
} from "./api/partitionFilesSchemas";

export interface PartitionData {
  graphData: { nodes: unknown[]; edges: unknown[]; metadata: TableMetadata };
  summary: PartitionFilesSummary;
}

const builtGraphDataSchema = z.object({
  nodes: z.array(z.unknown()),
  edges: z.array(z.unknown()),
});

interface DeleteFileNode {
  id: string;
  type: string;
  details: { applies_to_data_files?: unknown };
}

const isDeleteFileNode = (node: unknown): node is DeleteFileNode =>
  typeof node === "object" &&
  node !== null &&
  "id" in node &&
  "type" in node &&
  (node.type === FileType.POSITION_DELETE ||
    node.type === FileType.EQUALITY_DELETE);

const appliesToEdges = (nodes: unknown[]): unknown[] =>
  nodes.filter(isDeleteFileNode).flatMap((node) => {
    const targets = node.details.applies_to_data_files;
    return Array.isArray(targets)
      ? targets.map((target: unknown) => ({
          from: node.id,
          to: String(target),
          deletes_from_data_file: true,
          color: DELETION_VECTOR_CONNECTION_COLOR,
        }))
      : [];
  });

export const buildPartitionData = (
  partitionFiles: PartitionFiles,
  metadata: TableMetadata,
): PartitionData => {
  const graphData = builtGraphDataSchema.parse(
    buildGraphData({ ...partitionFiles, metadata }),
  );
  return {
    graphData: {
      nodes: graphData.nodes,
      edges: [...graphData.edges, ...appliesToEdges(graphData.nodes)],
      metadata,
    },
    summary: partitionFiles.summary,
  };
};

export const PartitionDataContext = createContext<PartitionData | null>(null);

export const usePartitionData = (): PartitionData => {
  const partitionData = useContext(PartitionDataContext);
  if (partitionData === null) {
    throw new Error("usePartitionData must be used under its provider");
  }
  return partitionData;
};
