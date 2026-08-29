import type { TimelineRow } from "./timelineRow";

/** A published write with no pointer yet is the table's own line — display it as main. */
export const displayBranchNameFor = (row: TimelineRow): string | null => {
  if (row.branchName !== null) {
    return row.branchName;
  }
  return row.kind === "published-write" ? "main" : null;
};
