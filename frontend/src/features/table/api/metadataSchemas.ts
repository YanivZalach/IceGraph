import { z } from "zod";

export const icebergIntegerSchema = z.union([
  z.number().int().refine(Number.isSafeInteger, "Unsafe Iceberg integer"),
  z.string().regex(/^-?\d+$/),
]);

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
  "current-schema-id": icebergIntegerSchema.nullish(),
  "default-spec-id": icebergIntegerSchema.nullish(),
  "default-sort-order-id": icebergIntegerSchema.nullish(),
  schemas: z.array(tableSchemaSchema).optional(),
  "partition-specs": z.array(partitionSpecSchema).optional(),
  "sort-orders": z.array(sortOrderSchema).optional(),
  properties: z.record(z.string(), z.unknown()).optional(),
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
