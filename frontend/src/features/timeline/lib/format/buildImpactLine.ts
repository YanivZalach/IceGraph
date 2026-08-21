import type { SnapshotSummary } from "../../api/nodeSchemas";
import { formatByteSize, parseBackendSizeToBytes } from "./backendSize";

const MINUS_SIGN = "−";
const IMPACT_SEGMENT_SEPARATOR = " · ";

const thousandsSeparatorFormatter = new Intl.NumberFormat("en-US");

/**
 * Iceberg's snapshot summary is an open string map — engines add arbitrary keys — so no schema
 * types its entries (https://iceberg.apache.org/spec/#snapshots). A missing or non-numeric value
 * counts as zero.
 */
const readSummaryCount = (summary: SnapshotSummary, key: string): number =>
  Number(summary[key]) || 0;

const formatPluralizedCount = (count: number, noun: "row" | "file"): string => {
  const pluralSuffix = count === 1 ? "" : "s";
  return `${thousandsSeparatorFormatter.format(count)} ${noun}${pluralSuffix}`;
};

export const buildSnapshotImpactSegments = (
  summary: SnapshotSummary,
): string[] => {
  const netAddedRowCount =
    readSummaryCount(summary, "added-records") -
    readSummaryCount(summary, "deleted-records");

  const changedFileCount =
    readSummaryCount(summary, "added-data-files") +
    readSummaryCount(summary, "deleted-data-files") +
    readSummaryCount(summary, "added-delete-files") +
    readSummaryCount(summary, "removed-delete-files");

  const netSizeChangeBytes = Math.abs(
    (parseBackendSizeToBytes(summary["added-files-size"]) ?? 0) -
      (parseBackendSizeToBytes(summary["removed-files-size"]) ?? 0),
  );

  const segments: string[] = [];

  if (netAddedRowCount !== 0) {
    const sign = netAddedRowCount > 0 ? "+" : MINUS_SIGN;
    segments.push(
      `${sign}${formatPluralizedCount(Math.abs(netAddedRowCount), "row")}`,
    );
  }

  if (changedFileCount !== 0) {
    segments.push(formatPluralizedCount(changedFileCount, "file"));
  }

  if (netSizeChangeBytes !== 0) {
    segments.push(formatByteSize(netSizeChangeBytes));
  }

  return segments;
};

export const joinImpactSegments = (segments: string[]): string =>
  segments.join(IMPACT_SEGMENT_SEPARATOR);
