import { z } from "zod";
import { snapshotIdSchema } from "./snapshotIdSchemas";

export const snapshotReferenceSchema = z.object({
  "snapshot-id": snapshotIdSchema,
  type: z.enum(["branch", "tag"]),
});

export type SnapshotReference = z.infer<typeof snapshotReferenceSchema>;
