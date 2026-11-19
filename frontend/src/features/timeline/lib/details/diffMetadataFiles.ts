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
  "pointed_table_statistics_files",
  "pointed_partition_statistics_files",
] as const;

type TrackedKey = (typeof TRACKED_KEYS)[number];

const hasFieldChanged = (before: unknown, after: unknown): boolean =>
  JSON.stringify(before) !== JSON.stringify(after);

// A metadata file without statistics may omit the list or write it empty; both mean none.
const fieldValue = (file: MetadataFileNode, key: TrackedKey): unknown =>
  key === "pointed_table_statistics_files" ||
  key === "pointed_partition_statistics_files"
    ? (file[key] ?? [])
    : file[key];

export const diffMetadataFiles = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
): FieldDiff[] =>
  TRACKED_KEYS.filter((key) =>
    hasFieldChanged(
      fieldValue(previousFile, key),
      fieldValue(currentFile, key),
    ),
  ).map((key) => ({
    key,
    before: fieldValue(previousFile, key),
    after: fieldValue(currentFile, key),
  }));
