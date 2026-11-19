import type { MetadataFileNode } from "../../api/nodeSchemas";

export interface FieldDiff {
  key: string;
  before: unknown;
  after: unknown;
}

const TRACKED_KEYS = [
  "snapshot_id",
  "last_sequence_number",
  "current_schema_id",
  "partition_spec_id",
  "sort_order_id",
  "refs",
  "properties",
  "pointed_snapshots_files",
  "format_version",
  "pointed_table_statistics_files",
  "pointed_partition_statistics_files",
] as const;

// Key order and a null written where another file omits the key are not changes.
const comparable = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map(comparable);
  }
  if (typeof value !== "object" || value === null) {
    return value ?? null;
  }
  return Object.entries(value)
    .filter(([, child]) => child !== null && child !== undefined)
    .sort(([firstKey], [secondKey]) => (firstKey < secondKey ? -1 : 1))
    .map(([key, child]) => [key, comparable(child)]);
};

const hasFieldChanged = (before: unknown, after: unknown): boolean =>
  JSON.stringify(comparable(before)) !== JSON.stringify(comparable(after));

export const diffMetadataFiles = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
): FieldDiff[] =>
  TRACKED_KEYS.filter((key) =>
    hasFieldChanged(previousFile[key], currentFile[key]),
  ).map((key) => ({
    key,
    before: previousFile[key],
    after: currentFile[key],
  }));
