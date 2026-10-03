import { z } from "zod";

export const icebergIntegerSchema = z.union([
  z.number().int().refine(Number.isSafeInteger, "Unsafe Iceberg integer"),
  z.string().regex(/^-?\d+$/),
]);

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

export const tableSchemaSchema = z.looseObject({
  "schema-id": icebergIntegerSchema,
  fields: z.array(z.unknown()).optional(),
  "identifier-field-ids": z.array(icebergIntegerSchema).optional(),
});

const partitionFieldSchema = z.looseObject({
  "field-id": icebergIntegerSchema.optional(),
  "source-id": icebergIntegerSchema.optional(),
  name: z.string().optional(),
  transform: z.string().optional(),
});

export const partitionSpecSchema = z.looseObject({
  "spec-id": icebergIntegerSchema,
  fields: z.array(partitionFieldSchema).optional(),
});

const sortFieldSchema = z.looseObject({
  "source-id": icebergIntegerSchema.optional(),
  transform: z.string().optional(),
  direction: z.string().optional(),
  "null-order": z.string().optional(),
});

export const sortOrderSchema = z.looseObject({
  "order-id": icebergIntegerSchema,
  fields: z.array(sortFieldSchema).optional(),
});

export const tableMetadataSchema = z.looseObject({
  "table-name": z.string().optional(),
  metadata_file_path: z.string().optional(),
  "table-uuid": z.string().optional(),
  location: z.string().optional(),
  "format-version": icebergIntegerSchema.optional(),
  "last-sequence-number": icebergIntegerSchema.optional(),
  "last-column-id": icebergIntegerSchema.optional(),
  "last-partition-id": icebergIntegerSchema.optional(),
  "last-updated-ms": icebergIntegerSchema.nullish(),
  "current-snapshot-id": icebergIntegerSchema.nullish(),
  "current-partition-statistics": z
    .object({
      file_path: z.string(),
      snapshot_id: icebergIntegerSchema,
      file_size_in_bytes: icebergIntegerSchema,
      partitions_count: icebergIntegerSchema.nullable(),
      partitions_with_deletes: icebergIntegerSchema.nullable(),
      partition_distribution: partitionDistributionSchema,
      errors: z.array(z.string()),
      warnings: z.array(z.string()),
    })
    .optional(),
  "current-schema-id": icebergIntegerSchema.nullish(),
  "default-spec-id": icebergIntegerSchema.nullish(),
  "default-sort-order-id": icebergIntegerSchema.nullish(),
  schemas: z.array(tableSchemaSchema).optional(),
  "partition-specs": z.array(partitionSpecSchema).optional(),
  "sort-orders": z.array(sortOrderSchema).optional(),
  properties: z.record(z.string(), z.unknown()).optional(),
  "current-snapshot": z
    .looseObject({
      "snapshot-id": icebergIntegerSchema,
      "timestamp-ms": icebergIntegerSchema.optional(),
      summary: z.record(z.string(), z.string()).optional(),
    })
    .nullish(),
  refs: z
    .record(
      z.string(),
      z.looseObject({
        type: z.string(),
        "snapshot-id": icebergIntegerSchema,
      }),
    )
    .optional(),
});

export type IcebergInteger = z.infer<typeof icebergIntegerSchema>;
export type TableMetadata = z.infer<typeof tableMetadataSchema>;
export type TableSchema = z.infer<typeof tableSchemaSchema>;
export type PartitionSpec = z.infer<typeof partitionSpecSchema>;
export type SortOrder = z.infer<typeof sortOrderSchema>;
