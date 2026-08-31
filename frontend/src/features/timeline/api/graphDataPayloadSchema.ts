import { z } from "zod";
import { tableMetadataSchema } from "./tableMetadataSchema";

/**
 * Field names throughout these schemas are the backend's own — its `snake_case` and Iceberg's
 * `kebab-case` — so a validation error names the key the payload carried.
 */
export const graphDataPayloadSchema = z.object({
  /** Validated one node at a time by `buildTimeline`: a malformed node costs one row, not the page. */
  nodes: z.array(z.unknown()),
  metadata: tableMetadataSchema,
});
