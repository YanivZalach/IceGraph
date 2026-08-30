import { displayBranchNameFor } from "./displayBranchName";
import type { TimelineRow } from "./timelineRow";

export type CommitKindFilter = "all" | "writes" | "metadata";

const matchesKind = (row: TimelineRow, kind: CommitKindFilter): boolean => {
  if (kind === "all") {
    return true;
  }
  const isWrite = row.kind === "published-write" || row.kind === "draft-write";
  return kind === "writes" ? isWrite : !isWrite;
};

const mentionsBranch = (row: TimelineRow, branchName: string): boolean => {
  const isMainsOwnMove = row.kind === "re-point" && branchName === "main";
  return (
    isMainsOwnMove ||
    displayBranchNameFor(row) === branchName ||
    row.movedToBranchName === branchName ||
    row.impact.some(
      (segment) =>
        segment.kind === "ref" &&
        segment.refType === "branch" &&
        segment.name === branchName,
    )
  );
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
      (branchName === null || mentionsBranch(row, branchName)),
  );
};
