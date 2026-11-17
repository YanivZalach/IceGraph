import { z } from "zod";
import { icebergIntegerSchema } from "../../table/api/metadataSchemas";
import { snapshotIdSchema } from "./snapshotIdSchemas";

export const snapshotReferenceSchema = z.object({
  "snapshot-id": snapshotIdSchema,
  type: z.enum(["branch", "tag"]),
  "min-snapshots-to-keep": icebergIntegerSchema.nullish(),
  "max-snapshot-age-ms": icebergIntegerSchema.nullish(),
  "max-ref-age-ms": icebergIntegerSchema.nullish(),
});

export type SnapshotReference = z.infer<typeof snapshotReferenceSchema>;
