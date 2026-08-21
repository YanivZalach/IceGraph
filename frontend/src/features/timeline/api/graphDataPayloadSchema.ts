import { z } from "zod";
import { tableMetadataSchema } from "./tableMetadataSchema";

/**
 * The `/api/v1/graph-data` response, parsed straight from the unmodified response text so ids stay
 * strings and timestamps stay UTC.
 *
 * Field names throughout these schemas are the backend's own — the `snake_case` of its dataclasses
 * and the `kebab-case` of Iceberg's metadata format — so a validation error names the key the
 * payload actually carried.
 */
export const graphDataPayloadSchema = z.object({
  /**
   * `nodes` mixes seven file types and is validated one node at a time by `buildTimeline`, so a
   * single malformed node costs one row rather than failing the whole array and blanking the page.
   */
  nodes: z.array(z.unknown()),
  metadata: tableMetadataSchema,
});
