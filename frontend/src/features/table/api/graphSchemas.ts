import { z } from "zod";
import { icebergIntegerSchema, tableMetadataSchema } from "./metadataSchemas";

export const partitionStatisticsRowSchema = z.record(z.string(), z.unknown());

const statisticValueSchema = z.union([z.number(), z.string()]).nullable();
const minAvgMaxSchema = z.object({
  min: statisticValueSchema,
  avg: statisticValueSchema,
  max: statisticValueSchema,
});

export const partitionDistributionSchema = z.object({
  data_record_count: minAvgMaxSchema.optional(),
  total_data_file_size_in_bytes: minAvgMaxSchema.optional(),
  data_file_count: minAvgMaxSchema.optional(),
  average_data_file_size_in_bytes: minAvgMaxSchema.optional(),
});

export const graphDataSchema = z.object({
  nodes: z.array(
    z.looseObject({
      file_path: z.string(),
      type: z.string(),
      virtual_read: z.boolean().optional(),
      snapshot_id: icebergIntegerSchema.nullish(),
      timestamp: z.string().nullish(),
      summary: z
        .record(
          z.string(),
          z.union([
            z.string(),
            z.number().int().refine(Number.isSafeInteger),
            z.null(),
          ]),
        )
        .nullish(),
      partitions_count: icebergIntegerSchema.nullish(),
      partitions_with_deletes: icebergIntegerSchema.nullish(),
      partition_distribution: partitionDistributionSchema.optional(),
      sampled_partitions: z.array(partitionStatisticsRowSchema).optional(),
    }),
  ),
  metadata: tableMetadataSchema,
  errors: z.record(z.string(), z.array(z.string())),
  warnings: z.record(z.string(), z.array(z.string())),
});

export const graphJobSubmissionSchema = z.object({
  key: z.string(),
  status: z.literal("processing"),
  "X-IceGraph-Job-Token": z.string(),
});

export const graphProgressSchema = z.object({
  key: z.string(),
  status: z.literal("processing"),
  stages: z
    .record(z.string(), z.enum(["pending", "in_progress", "done"]))
    .nullable()
    .optional(),
});

export const graphJobPollResponseSchema = z.union([
  graphDataSchema,
  graphProgressSchema,
]);

export const graphMetadataFileSchema = z.object({
  metadata_file: z.string(),
});

export type GraphData = z.infer<typeof graphDataSchema>;
export type GraphProgress = z.infer<typeof graphProgressSchema>;
export type GraphStages = NonNullable<GraphProgress["stages"]>;

export type GraphNode = GraphData["nodes"][number];
