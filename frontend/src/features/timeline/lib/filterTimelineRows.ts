import { z } from "zod";
import { displayBranchNameFor } from "./displayBranchName";
import type { TimelineRow } from "./timelineRow";

export const commitKindFilterSchema = z.enum(["all", "writes", "metadata"]);
export type CommitKindFilter = z.infer<typeof commitKindFilterSchema>;

const matchesKind = (row: TimelineRow, kind: CommitKindFilter): boolean => {
  if (kind === "all") {
    return true;
  }
  const isWrite = row.kind === "published-write" || row.kind === "draft-write";
  return kind === "writes" ? isWrite : !isWrite;
};

export const filterTimelineRows = (
  rows: TimelineRow[],
  kind: CommitKindFilter,
  branchName: string | null,
): TimelineRow[] => {
  if (kind === "all" && branchName === null) {
    return rows;
  }
  return rows.filter(
    (row) =>
      row.kind !== "boundary" &&
      matchesKind(row, kind) &&
      (branchName === null || displayBranchNameFor(row) === branchName),
  );
};
