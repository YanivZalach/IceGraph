import { z } from "zod";
import { icebergIntegerSchema, tableMetadataSchema } from "./metadataSchemas";

export const graphDataSchema = z.object({
  nodes: z.array(
    z.looseObject({
      file_path: z.string(),
      type: z.string(),
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
    }),
  ),
  metadata: tableMetadataSchema,
  errors: z.record(z.string(), z.unknown()),
  warnings: z.record(z.string(), z.unknown()),
});

export const graphJobSubmissionSchema = z.object({
  key: z.string(),
  status: z.literal("processing"),
  "X-IceGraph-Job-Token": z.string(),
});

export const graphProgressSchema = z.object({
  key: z.string(),
  status: z.literal("processing"),
  stages: z.record(z.string(), z.string()).nullable().optional(),
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

export type GraphNode = GraphData["nodes"][number];
