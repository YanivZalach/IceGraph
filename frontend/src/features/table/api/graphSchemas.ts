import { z } from "zod";
import { icebergIntegerSchema, tableMetadataSchema } from "./metadataSchemas";

export const partitionStatisticsRowSchema = z.record(z.string(), z.unknown());

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
