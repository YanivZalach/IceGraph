import { z } from "zod";
import {
  optionalSnapshotIdSchema,
  snapshotIdSchema,
} from "./snapshotIdSchemas";
import { snapshotReferenceSchema } from "./snapshotReferenceSchema";
import { utcTimestampSchema } from "./utcTimestampSchema";

const pointedSnapshotFileSchema = z.object({
  "snapshot-id": snapshotIdSchema,
  "manifest-list": z.string().nullish(),
});

/**
 * `error`/`warning` are optional only because the MSW mock omits them; the live backend always
 * sends both. TODO: add them to the mock nodes, then require them here.
 */
const baseNodeFields = {
  file_path: z.string(),
  child_files: z.array(z.string()),
  error: z.string().nullish(),
  warning: z.string().nullish(),
};

export const metadataFileNodeSchema = z.object({
  ...baseNodeFields,
  type: z.enum(["main_metadata", "metadata"]),
  timestamp: utcTimestampSchema,
  snapshot_id: optionalSnapshotIdSchema,
  previous_file: z.string().nullable(),
  last_sequence_number: z.number().nullable(),
  partition_spec_id: z.number().nullable(),
  current_schema_id: z.number().nullable(),
  sort_order_id: z.number().nullable(),
  refs: z.record(z.string(), snapshotReferenceSchema),
  properties: z.record(z.string(), z.string()),
  pointed_snapshots_files: z.array(pointedSnapshotFileSchema).nullable(),
  pointed_metadata_log_count: z.number().nullable(),
});

export const snapshotNodeSchema = z.object({
  ...baseNodeFields,
  type: z.literal("snapshot"),
  timestamp: utcTimestampSchema,
  snapshot_id: snapshotIdSchema,
  parent_id: optionalSnapshotIdSchema,
  operation: z.string().nullable(),
  operation_description: z.string().nullable(),
  action_link: z.string().nullish(),
  summary: z.record(z.string(), z.string()),
});

/** Routes a node to its schema; every other file type is ignored by this feature. */
export const nodeTypeSchema = z.object({ type: z.string() });

export type MetadataFileNode = z.infer<typeof metadataFileNodeSchema>;
export type SnapshotNode = z.infer<typeof snapshotNodeSchema>;
export type SnapshotSummary = SnapshotNode["summary"];
export type SnapshotRefs = MetadataFileNode["refs"];
export type TableProperties = MetadataFileNode["properties"];
