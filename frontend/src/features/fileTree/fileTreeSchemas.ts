import { z } from "zod";

const identifierSchema = z
  .union([z.string(), z.number(), z.bigint()])
  .transform((value) => String(value));

const nullableIdentifierSchema = identifierSchema.nullable().optional();

const numericValueSchema = z
  .union([z.number(), z.string()])
  .transform((value) => Number(value))
  .pipe(z.number());

export const fileDetailsSchema = z
  .object({
    error: z.string().nullish(),
    timestamp: z.union([z.string(), z.number()]).optional(),
    snapshot_id: nullableIdentifierSchema,
    parent_id: nullableIdentifierSchema,
    partition: z.string().optional(),
    earliest_appearing_snapshot_id: nullableIdentifierSchema,
    earliest_appearing_snapshot_timestamp: z.string().nullish(),
    format: z.string().optional(),
    size_gb: numericValueSchema.optional(),
    row_count: numericValueSchema.optional(),
  })
  .catchall(z.unknown());

export const graphNodeSchema = z.object({
  id: identifierSchema,
  label: z.string().optional(),
  type: z.string(),
  details: fileDetailsSchema.default({}),
});

export const graphEdgeSchema = z.object({
  from: identifierSchema,
  to: identifierSchema,
  is_deleted: z.boolean().optional(),
});

const metadataReferenceSchema = z
  .object({
    type: z.string(),
    "snapshot-id": identifierSchema,
  })
  .catchall(z.unknown());

const metadataSchema = z
  .object({
    refs: z.record(z.string(), metadataReferenceSchema).optional(),
  })
  .catchall(z.unknown());

export const fileTreeContextSchema = z.object({
  nodes: z.array(graphNodeSchema).default([]),
  edges: z.array(graphEdgeSchema).default([]),
  metadata: metadataSchema.nullable().optional(),
});

export type FileDetails = z.infer<typeof fileDetailsSchema>;
export type GraphEdge = z.infer<typeof graphEdgeSchema>;
export type GraphNode = z.infer<typeof graphNodeSchema>;
export type FileTreeContext = z.infer<typeof fileTreeContextSchema>;
