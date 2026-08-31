import { z } from "zod";
import { parseUtcTimestampToMillis } from "../lib/format/formatTimelineTime";

/**
 * Parsed here so `timestamp` holds epoch milliseconds everywhere in the feature; a file whose
 * instant cannot be read fails validation and is skipped.
 */
export const utcTimestampSchema = z
  .string()
  .transform((rawTimestamp, context) => {
    const timestampMs = parseUtcTimestampToMillis(rawTimestamp);

    if (timestampMs === null) {
      context.addIssue({
        code: "custom",
        message: `Unreadable UTC timestamp: ${rawTimestamp}`,
      });
      return z.NEVER;
    }

    return timestampMs;
  });
