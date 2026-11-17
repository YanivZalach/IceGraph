import { z } from "zod";
import { parseUtcTimestampToMillis } from "../lib/format/formatTimelineTime";

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
