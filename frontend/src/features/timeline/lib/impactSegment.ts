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

const CHANGE_KEY_PATTERN = /^(added|deleted|removed)-/;

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

/**
 * Iceberg omits zero deltas, so a write proves "no rows" only when the engine wrote the table
 * totals and no file, delete or deletion-vector delta: delete files change rows without touching
 * added-records or deleted-records.
 */
export const isProvablyUnchangedWrite = (summary: SnapshotSummary): boolean =>
  summary["total-records"] !== undefined &&
  !Object.entries(summary).some(
    ([key, value]) =>
      CHANGE_KEY_PATTERN.test(key) && value !== "0" && value !== "",
  );

const SINGULAR_UNIT = { rows: "row", files: "file" } as const;

const COUNT_SIGN_FORMATTER = new Intl.NumberFormat("en-US");

export const impactPlainText = (segments: ImpactSegment[]): string =>
  segments
    .map((segment) => {
      if (segment.kind === "text") {
        return segment.text;
      }
      if (segment.kind === "ref") {
        return `${segment.refType} ${segment.name} ${segment.action}`;
      }
      const parts = [
        segment.added > 0n
          ? `+${COUNT_SIGN_FORMATTER.format(segment.added)}`
          : "",
        segment.removed > 0n
          ? `−${COUNT_SIGN_FORMATTER.format(segment.removed)}`
          : "",
      ].filter((part) => part !== "");
      const unit =
        segment.added + segment.removed === 1n
          ? SINGULAR_UNIT[segment.unit]
          : segment.unit;
      return `${parts.join(" ")} ${unit}`;
    })
    .join(" · ");

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

  if (!isRewrite && isProvablyUnchangedWrite(summary)) {
    return [impactText("no rows changed")];
  }

  return [];
};
