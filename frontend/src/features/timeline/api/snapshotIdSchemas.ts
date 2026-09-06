import { z } from "zod";

/** Iceberg writes this in `current-snapshot-id` when a table has no live snapshot yet. */
const NO_CURRENT_SNAPSHOT_SENTINEL = "-1";

export const snapshotIdSchema = z
  .union([z.string(), z.number()])
  .transform((rawSnapshotId) => String(rawSnapshotId));

export const optionalSnapshotIdSchema = z
  .union([z.string(), z.number(), z.null()])
  .transform((rawSnapshotId) =>
    rawSnapshotId === null ||
    String(rawSnapshotId) === NO_CURRENT_SNAPSHOT_SENTINEL
      ? null
      : String(rawSnapshotId),
  );
