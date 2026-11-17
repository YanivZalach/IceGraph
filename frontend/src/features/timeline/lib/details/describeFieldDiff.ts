import type { FieldDiff } from "./diffMetadataFiles";

export type DiffEntryStatus = "added" | "removed" | "changed" | "unchanged";

export interface DiffEntry {
  label: string;
  status: DiffEntryStatus;
  before: string | null;
  after: string | null;
}

export interface DescribedFieldDiff {
  key: string;
  title: string;
  changedEntries: DiffEntry[];
  unchangedEntries: DiffEntry[];
  rawBefore: string;
  rawAfter: string;
}

const FIELD_TITLES: Record<string, string> = {
  snapshot_id: "Current snapshot",
  last_sequence_number: "Last sequence number",
  current_schema_id: "Current schema ID",
  partition_spec_id: "Default partition spec ID",
  sort_order_id: "Default sort order ID",
  refs: "Branches and tags",
  properties: "Table properties",
  pointed_snapshots_files: "Snapshots",
};

const LABEL_SEPARATOR = " · ";
const EMPTY_RAW_VALUE = "–";

const joinLabel = (prefix: string, part: string): string =>
  prefix === "" ? part : `${prefix}${LABEL_SEPARATOR}${part}`;

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const leafText = (value: unknown): string => {
  if (typeof value === "string") {
    return value;
  }
  if (
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return value.toString();
  }
  return JSON.stringify(value);
};

const arrayItemLabel = (item: unknown, index: number): string => {
  if (isRecord(item) && typeof item["snapshot-id"] === "string") {
    return `snapshot ${item["snapshot-id"]}`;
  }
  return `#${index.toString()}`;
};

/** Every leaf keeps its exact text, so the readable view carries the same facts as the JSON. */
const flattenLeaves = (
  value: unknown,
  prefix: string,
  leaves: Map<string, string>,
): void => {
  if (value === null || value === undefined) {
    if (prefix !== "") {
      leaves.set(prefix, "null");
    }
    return;
  }
  if (Array.isArray(value)) {
    const usedLabels = new Set<string>();
    value.forEach((item, index) => {
      const itemLabel = arrayItemLabel(item, index);
      const uniqueLabel = usedLabels.has(itemLabel)
        ? `${itemLabel} #${index.toString()}`
        : itemLabel;
      usedLabels.add(uniqueLabel);
      flattenLeaves(item, joinLabel(prefix, uniqueLabel), leaves);
    });
    return;
  }
  if (isRecord(value)) {
    for (const [key, child] of Object.entries(value)) {
      flattenLeaves(child, joinLabel(prefix, key), leaves);
    }
    return;
  }
  leaves.set(prefix, leafText(value));
};

const leavesOf = (value: unknown): Map<string, string> => {
  const leaves = new Map<string, string>();
  const isScalar =
    value === null || value === undefined || typeof value !== "object";
  if (isScalar) {
    if (value !== null && value !== undefined) {
      leaves.set("", leafText(value));
    }
    return leaves;
  }
  flattenLeaves(value, "", leaves);
  return leaves;
};

export const formatRawValue = (value: unknown): string => {
  if (value === null || value === undefined) {
    return EMPTY_RAW_VALUE;
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return value.toString();
  }
  return JSON.stringify(value, null, 2);
};

const statusOf = (
  before: string | undefined,
  after: string | undefined,
): DiffEntryStatus => {
  if (before === undefined) {
    return "added";
  }
  if (after === undefined) {
    return "removed";
  }
  return before === after ? "unchanged" : "changed";
};

export const describeFieldDiff = (diff: FieldDiff): DescribedFieldDiff => {
  const beforeLeaves = leavesOf(diff.before);
  const afterLeaves = leavesOf(diff.after);
  const labels = [
    ...afterLeaves.keys(),
    ...[...beforeLeaves.keys()].filter((label) => !afterLeaves.has(label)),
  ];

  const changedEntries: DiffEntry[] = [];
  const unchangedEntries: DiffEntry[] = [];
  for (const label of labels) {
    const before = beforeLeaves.get(label);
    const after = afterLeaves.get(label);
    const entry: DiffEntry = {
      label,
      status: statusOf(before, after),
      before: before ?? null,
      after: after ?? null,
    };
    if (entry.status === "unchanged") {
      unchangedEntries.push(entry);
    } else {
      changedEntries.push(entry);
    }
  }

  const rawBefore = formatRawValue(diff.before);
  const rawAfter = formatRawValue(diff.after);
  // An empty value replaced by another empty one ({} to null) has no leaves to compare.
  if (changedEntries.length === 0) {
    changedEntries.push({
      label: "",
      status: "changed",
      before: rawBefore,
      after: rawAfter,
    });
  }

  return {
    key: diff.key,
    title: FIELD_TITLES[diff.key] ?? diff.key,
    changedEntries,
    unchangedEntries,
    rawBefore,
    rawAfter,
  };
};
