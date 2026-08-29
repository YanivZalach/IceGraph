import type { SnapshotSummary } from "../api/nodeSchemas";
import { parseBackendSizeToBytes } from "./format/backendSize";

export type ImpactSegment =
  | { kind: "count"; added: number; removed: number; unit: "rows" | "files" }
  | { kind: "size"; netBytes: number }
  | { kind: "ref"; refType: "branch" | "tag"; name: string; action: string }
  | { kind: "text"; text: string };

export const impactText = (text: string): ImpactSegment => ({
  kind: "text",
  text,
});

/**
 * Iceberg's snapshot summary is an open string map — engines add arbitrary keys — so no schema
 * types its entries (https://iceberg.apache.org/spec/#snapshots). A missing or non-numeric value
 * counts as zero.
 */
const readSummaryCount = (summary: SnapshotSummary, key: string): number =>
  Number(summary[key]) || 0;

export const readSnapshotImpact = (
  summary: SnapshotSummary,
): ImpactSegment[] => {
  const addedRows = readSummaryCount(summary, "added-records");
  const removedRows = readSummaryCount(summary, "deleted-records");

  const addedFiles =
    readSummaryCount(summary, "added-data-files") +
    readSummaryCount(summary, "added-delete-files");
  const removedFiles =
    readSummaryCount(summary, "deleted-data-files") +
    readSummaryCount(summary, "removed-delete-files");

  const addedBytes = parseBackendSizeToBytes(summary["added-files-size"]) ?? 0;
  const removedBytes =
    parseBackendSizeToBytes(summary["removed-files-size"]) ?? 0;
  const netBytes = addedBytes - removedBytes;

  const hasRowChange = addedRows > 0 || removedRows > 0;
  const hasFileChange = addedFiles > 0 || removedFiles > 0;
  const hasSizeChange = netBytes !== 0;

  const segments: ImpactSegment[] = [];

  if (hasRowChange) {
    segments.push({
      kind: "count",
      added: addedRows,
      removed: removedRows,
      unit: "rows",
    });
  }

  if (hasFileChange) {
    segments.push({
      kind: "count",
      added: addedFiles,
      removed: removedFiles,
      unit: "files",
    });
  }

  if (hasSizeChange) {
    segments.push({ kind: "size", netBytes });
  }

  return segments;
};
