import { z } from "zod";
import {
  partitionSchema,
  partitionStatisticsRowSchema,
} from "../table/api/graphSchemas";
import {
  icebergIntegerSchema,
  partitionDistributionSchema,
  tableMetadataSchema,
} from "../table/api/metadataSchemas";

const readableMetricsSchema = z.record(
  z.string(),
  z.looseObject({ field_type: z.string() }),
);

export const graphNodeDetailsSchema = z.looseObject({
  type: z.string().optional(),
  file_path: z.string().optional(),
  errors: z.array(z.string()).nullish(),
  warnings: z.array(z.string()).nullish(),
  virtual_read: z.boolean().optional(),
  file_size_in_bytes: z.union([z.string(), z.number()]).nullish(),
  partition: partitionSchema.nullish(),
  partitions: z.array(partitionSchema).nullish(),
  partitions_count: icebergIntegerSchema.nullish(),
  partition_distribution: partitionDistributionSchema.nullish(),
  sampled_partitions: z.array(partitionStatisticsRowSchema).nullish(),
  readable_metrics: readableMetricsSchema.nullish(),
});

const graphFileNodeSchema = z.object({
  id: z.string(),
  label: z.string(),
  type: z.string(),
  details: graphNodeDetailsSchema,
  color: z.string(),
  level: z.number(),
});

const graphFileEdgeSchema = z.object({
  from: z.string(),
  to: z.string(),
  color: z.string().optional(),
  curvature: z.number().optional(),
  branch_names: z.string().optional(),
  deletes_from_data_file: z.boolean().optional(),
});

export const graphViewDataSchema = z.object({
  nodes: z.array(graphFileNodeSchema),
  edges: z.array(graphFileEdgeSchema),
  metadata: tableMetadataSchema,
});

export type GraphNodeDetails = z.infer<typeof graphNodeDetailsSchema>;
export type GraphFileNode = z.infer<typeof graphFileNodeSchema>;
export type GraphFileEdge = z.infer<typeof graphFileEdgeSchema>;
export type GraphViewData = z.infer<typeof graphViewDataSchema>;
