import { z } from "zod";
import { graphDataSchema } from "../../table/api/graphSchemas";
import { icebergIntegerSchema } from "../../table/api/metadataSchemas";

const partitionFilterFieldSchema = z.object({
  name: z.string(),
  field_id: icebergIntegerSchema,
  transform: z.string(),
  source_name: z.string().nullable(),
  result_type: z.string(),
  value: z.string(),
});

export const partitionFilesSummarySchema = z.object({
  table_name: z.string(),
  snapshot_id: icebergIntegerSchema,
  snapshot_timestamp: z.string().nullable(),
  format_version: icebergIntegerSchema,
  filters: z.array(partitionFilterFieldSchema),
  undecided_spec_ids: z.array(icebergIntegerSchema),
  undecided_files_count: z.number().int(),
});

export const partitionFilesSchema = z.object({
  nodes: graphDataSchema.shape.nodes,
  summary: partitionFilesSummarySchema,
  errors: z.record(z.string(), z.array(z.string())),
  warnings: z.record(z.string(), z.array(z.string())),
});

export const partitionFilesPollResponseSchema = z.union([
  partitionFilesSchema,
  z.object({
    key: z.string(),
    status: z.literal("processing"),
    stages: z
      .record(z.string(), z.enum(["pending", "in_progress", "done"]))
      .nullable()
      .optional(),
  }),
]);

export const partitionFilesErrorBodySchema = z.object({
  error: z.string(),
  spark_sql: z.string().nullish(),
});

export type PartitionFiles = z.infer<typeof partitionFilesSchema>;
export type PartitionFilesSummary = z.infer<typeof partitionFilesSummarySchema>;
