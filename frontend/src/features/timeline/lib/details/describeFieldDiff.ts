import type { FieldDiff } from "./diffMetadataFiles";

export type DiffLineTone = "removed" | "added" | "same";

export interface DiffLine {
  tone: DiffLineTone;
  text: string;
}

export interface DescribedFieldDiff {
  key: string;
  openBracket: string | null;
  closeBracket: string | null;
  lines: DiffLine[];
}

export const FIELD_TITLES: Record<string, string> = {
  snapshot_id: "Current snapshot",
  last_sequence_number: "Last sequence number",
  current_schema_id: "Current schema ID",
  partition_spec_id: "Default partition spec ID",
  sort_order_id: "Default sort order ID",
  refs: "Branches and tags",
  properties: "Table properties",
  pointed_snapshots_files: "Snapshots",
  pointed_table_statistics_files: "Table statistics",
  pointed_partition_statistics_files: "Partition statistics",
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isMissing = (value: unknown): boolean =>
  value === null || value === undefined;

const valueText = (value: unknown): string =>
  isMissing(value)
    ? "null"
    : typeof value === "string"
      ? value
      : JSON.stringify(value);

const MAX_LCS_CELLS = 250_000;

const lcsLines = (beforeKeys: string[], afterKeys: string[]): DiffLine[] => {
  const n = beforeKeys.length;
  const m = afterKeys.length;
  const width = m + 1;
  const common = new Array<number>((n + 1) * width).fill(0);
  const commonAt = (i: number, j: number): number => common[i * width + j] ?? 0;
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      common[i * width + j] =
        beforeKeys[i] === afterKeys[j]
          ? commonAt(i + 1, j + 1) + 1
          : Math.max(commonAt(i + 1, j), commonAt(i, j + 1));
    }
  }

  const lines: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < n || j < m) {
    const beforeKey = beforeKeys[i];
    const afterKey = afterKeys[j];
    if (beforeKey !== undefined && beforeKey === afterKey) {
      lines.push({ tone: "same", text: beforeKey });
      i++;
      j++;
    } else if (
      beforeKey !== undefined &&
      (afterKey === undefined || commonAt(i + 1, j) >= commonAt(i, j + 1))
    ) {
      lines.push({ tone: "removed", text: beforeKey });
      i++;
    } else if (afterKey !== undefined) {
      lines.push({ tone: "added", text: afterKey });
      j++;
    }
  }

  return lines;
};

// Too large for an exact diff: a linear pass that still lists every item once.
const linearLines = (beforeKeys: string[], afterKeys: string[]): DiffLine[] => {
  const beforeSet = new Set(beforeKeys);
  const afterSet = new Set(afterKeys);
  const lines: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < beforeKeys.length || j < afterKeys.length) {
    const beforeKey = beforeKeys[i];
    const afterKey = afterKeys[j];
    if (beforeKey !== undefined && beforeKey === afterKey) {
      lines.push({ tone: "same", text: beforeKey });
      i++;
      j++;
    } else if (
      beforeKey !== undefined &&
      (afterKey === undefined || !afterSet.has(beforeKey))
    ) {
      lines.push({ tone: "removed", text: beforeKey });
      i++;
    } else if (
      afterKey !== undefined &&
      (beforeKey === undefined || !beforeSet.has(afterKey))
    ) {
      lines.push({ tone: "added", text: afterKey });
      j++;
    } else if (beforeKey !== undefined && afterKey !== undefined) {
      lines.push({ tone: "removed", text: beforeKey });
      lines.push({ tone: "added", text: afterKey });
      i++;
      j++;
    }
  }

  return lines;
};

const listLines = (before: unknown[], after: unknown[]): DiffLine[] => {
  const beforeKeys = before.map((item) => JSON.stringify(item));
  const afterKeys = after.map((item) => JSON.stringify(item));
  let start = 0;
  while (
    start < beforeKeys.length &&
    start < afterKeys.length &&
    beforeKeys[start] === afterKeys[start]
  ) {
    start++;
  }
  let end = 0;
  while (
    end < beforeKeys.length - start &&
    end < afterKeys.length - start &&
    beforeKeys[beforeKeys.length - 1 - end] ===
      afterKeys[afterKeys.length - 1 - end]
  ) {
    end++;
  }

  const middleBefore = beforeKeys.slice(start, beforeKeys.length - end);
  const middleAfter = afterKeys.slice(start, afterKeys.length - end);
  const middle =
    (middleBefore.length + 1) * (middleAfter.length + 1) > MAX_LCS_CELLS
      ? linearLines(middleBefore, middleAfter)
      : lcsLines(middleBefore, middleAfter);
  const same = (text: string): DiffLine => ({ tone: "same", text });

  return [
    ...beforeKeys.slice(0, start).map(same),
    ...middle,
    ...beforeKeys.slice(beforeKeys.length - end).map(same),
  ];
};

const objectLines = (
  before: Record<string, unknown>,
  after: Record<string, unknown>,
): DiffLine[] => {
  const keys = [
    ...new Set([...Object.keys(before), ...Object.keys(after)]),
  ].sort();
  const lines: DiffLine[] = [];
  for (const key of keys) {
    const beforeText = key in before ? JSON.stringify(before[key]) : null;
    const afterText = key in after ? JSON.stringify(after[key]) : null;
    if (beforeText === afterText) {
      lines.push({ tone: "same", text: `"${key}": ${beforeText ?? ""}` });
      continue;
    }
    if (beforeText !== null) {
      lines.push({ tone: "removed", text: `"${key}": ${beforeText}` });
    }
    if (afterText !== null) {
      lines.push({ tone: "added", text: `"${key}": ${afterText}` });
    }
  }

  return lines;
};

const scalarLines = (before: unknown, after: unknown): DiffLine[] => [
  { tone: "removed", text: valueText(before) },
  { tone: "added", text: valueText(after) },
];

const hasChange = (lines: DiffLine[]): boolean =>
  lines.some((line) => line.tone !== "same");

// A missing list or object compares as an empty one, unless that hides the change (null to []).
export const describeFieldDiff = (diff: FieldDiff): DescribedFieldDiff => {
  const { key, before, after } = diff;
  const isList =
    (Array.isArray(before) || isMissing(before)) &&
    (Array.isArray(after) || isMissing(after)) &&
    !(isMissing(before) && isMissing(after));
  if (isList) {
    const lines = listLines(
      Array.isArray(before) ? before : [],
      Array.isArray(after) ? after : [],
    );
    if (hasChange(lines)) {
      return { key, openBracket: "[", closeBracket: "]", lines };
    }
  }

  const isObject =
    (isRecord(before) || isMissing(before)) &&
    (isRecord(after) || isMissing(after)) &&
    !(isMissing(before) && isMissing(after));
  if (isObject) {
    const lines = objectLines(
      isRecord(before) ? before : {},
      isRecord(after) ? after : {},
    );
    if (hasChange(lines)) {
      return { key, openBracket: "{", closeBracket: "}", lines };
    }
  }

  return {
    key,
    openBracket: null,
    closeBracket: null,
    lines: scalarLines(before, after),
  };
};
