import { createContext, useContext } from "react";
import { z } from "zod";
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
      edges: graphData.edges,
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
