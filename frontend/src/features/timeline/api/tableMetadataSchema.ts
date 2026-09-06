import { z } from "zod";
import { icebergStructFieldSchema } from "./icebergTypeSchemas";
import { optionalSnapshotIdSchema } from "./snapshotIdSchemas";
import { snapshotReferenceSchema } from "./snapshotReferenceSchema";

/**
 * Iceberg keeps these arrays append-only, so the newest loaded file holds every definition the
 * ids point at.
 */
export const tableMetadataSchema = z.object({
  "table-name": z.string().nullish(),
  "table-uuid": z.string().nullish(),
  location: z.string().nullish(),
  "format-version": z.number().nullish(),
  "last-updated-ms": z.number().nullish(),
  "last-column-id": z.number().nullish(),
  "last-partition-id": z.number().nullish(),
  "last-sequence-number": z.number().nullish(),
  "current-schema-id": z.number().nullish(),
  "current-snapshot-id": optionalSnapshotIdSchema.nullish(),
  "default-spec-id": z.number().nullish(),
  "default-sort-order-id": z.number().nullish(),
  schemas: z
    .array(
      z.object({
        "schema-id": z.number(),
        type: z.string().nullish(),
        fields: z.array(icebergStructFieldSchema),
        "identifier-field-ids": z.array(z.number()).nullish(),
      }),
    )
    .nullish(),
  "partition-specs": z
    .array(
      z.object({
        "spec-id": z.number(),
        fields: z.array(
          z.object({
            name: z.string(),
            transform: z.string(),
            "source-id": z.number(),
            "field-id": z.number().nullish(),
          }),
        ),
      }),
    )
    .nullish(),
  "sort-orders": z
    .array(
      z.object({
        "order-id": z.number(),
        fields: z.array(
          z.object({
            transform: z.string(),
            "source-id": z.number(),
            direction: z.string().nullish(),
            "null-order": z.string().nullish(),
          }),
        ),
      }),
    )
    .nullish(),
  properties: z.record(z.string(), z.string()).nullish(),
  refs: z.record(z.string(), snapshotReferenceSchema).nullish(),
});

export type TableMetadata = z.infer<typeof tableMetadataSchema>;
