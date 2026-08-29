import { z } from "zod";

export const graphDataSchema = z.object({
  nodes: z.array(
    z.looseObject({
      file_path: z.string(),
      type: z.string(),
    }),
  ),
  metadata: z.record(z.string(), z.unknown()),
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
