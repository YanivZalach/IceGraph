import type { CommitRow, TimelineRow } from "./timelineRow";

export type CommitKindFilter = "all" | "writes" | "metadata";

const matchesKind = (row: CommitRow, kind: CommitKindFilter): boolean => {
  if (kind === "all") {
    return true;
  }
  if (row.kind === "unknown-change") {
    return false;
  }
  const isWrite = row.kind === "published-write" || row.kind === "draft-write";
  return kind === "writes" ? isWrite : !isWrite;
};

const mentionsBranch = (row: CommitRow, branchName: string): boolean => {
  const isMainsOwnMove = row.kind === "re-point" && branchName === "main";
  return (
    isMainsOwnMove ||
    row.branchName === branchName ||
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
      row.kind !== "gap" &&
      matchesKind(row, kind) &&
      (branchName === null || mentionsBranch(row, branchName)),
  );
};
