import type { SnapshotSummary } from "../api/nodeSchemas";

export type ImpactSegment =
  | { kind: "count"; added: bigint; removed: bigint; unit: "rows" | "files" }
  | { kind: "ref"; refType: "branch" | "tag"; name: string; action: string }
  | { kind: "text"; text: string };

export const impactText = (text: string): ImpactSegment => ({
  kind: "text",
  text,
});

const DECIMAL_INTEGER_PATTERN = /^\d+$/;

/**
 * Iceberg's snapshot summary is an open string map — engines add arbitrary keys
 * (https://iceberg.apache.org/spec/#snapshots) — so a missing or non-numeric value counts as zero.
 */
const readSummaryCount = (summary: SnapshotSummary, key: string): bigint => {
  const value = summary[key];
  return value !== undefined && DECIMAL_INTEGER_PATTERN.test(value)
    ? BigInt(value)
    : 0n;
};

export interface RowCounts {
  addedRows: bigint;
  removedRows: bigint;
}

export const readRowCounts = (summary: SnapshotSummary): RowCounts => ({
  addedRows: readSummaryCount(summary, "added-records"),
  removedRows: readSummaryCount(summary, "deleted-records"),
});

export const readSnapshotImpact = (
  summary: SnapshotSummary,
  operation: string | null,
): ImpactSegment[] => {
  const { addedRows, removedRows } = readRowCounts(summary);
  const isRewrite = operation === "replace";

  if (!isRewrite && (addedRows > 0n || removedRows > 0n)) {
    return [
      { kind: "count", added: addedRows, removed: removedRows, unit: "rows" },
    ];
  }

  // A rewrite changes no rows (https://iceberg.apache.org/spec/#snapshots);
  // the number of files is its only visible effect.
  const addedFiles =
    readSummaryCount(summary, "added-data-files") +
    readSummaryCount(summary, "added-delete-files");
  const removedFiles =
    readSummaryCount(summary, "deleted-data-files") +
    readSummaryCount(summary, "removed-delete-files");

  if (isRewrite && (addedFiles > 0n || removedFiles > 0n)) {
    return [
      {
        kind: "count",
        added: addedFiles,
        removed: removedFiles,
        unit: "files",
      },
    ];
  }

  return [];
};
