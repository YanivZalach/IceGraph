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
] as const;

const hasFieldChanged = (before: unknown, after: unknown): boolean =>
  JSON.stringify(before) !== JSON.stringify(after);

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
