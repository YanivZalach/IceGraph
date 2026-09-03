import type { SnapshotSummary } from "../../api/nodeSchemas";
import { parseBackendSizeToBytes } from "../format/backendSize";
import { humanizeValue } from "../format/humanizeSummary";
import type { SummaryEntry } from "./groupSnapshotSummary";

export type ChangeSide = "added" | "removed";

export interface ChangeCountCell {
  text: string;
  emphasis: ChangeSide | null;
}

export interface ChangeCountRow {
  metric: string;
  added: ChangeCountCell | null;
  removed: ChangeCountCell | null;
}

export interface ChangeCounts {
  counts: ChangeCountRow[];
  rest: SummaryEntry[];
}

const SIGN_BY_SIDE: Record<ChangeSide, string> = { added: "+", removed: "−" };

const ALL_ZEROS_PATTERN = /^0+$/;

const isZeroValue = (value: string): boolean =>
  ALL_ZEROS_PATTERN.test(value) || parseBackendSizeToBytes(value) === 0;

const sideOfKey = (key: string): ChangeSide | null => {
  if (key.startsWith("added-")) {
    return "added";
  }
  if (key.startsWith("deleted-") || key.startsWith("removed-")) {
    return "removed";
  }
  return null;
};

const countCell = (
  value: string | null,
  side: ChangeSide,
): ChangeCountCell | null => {
  if (value === null) {
    return null;
  }
  const text = humanizeValue(value, false);
  return isZeroValue(value)
    ? { text, emphasis: null }
    : { text: `${SIGN_BY_SIDE[side]}${text}`, emphasis: side };
};

// Iceberg spells the negative side either `deleted-` or `removed-`.
export const buildChangeCounts = (entries: SummaryEntry[]): ChangeCounts => {
  const sidesByMetric = new Map<
    string,
    { added: string | null; removed: string | null }
  >();
  const rest: SummaryEntry[] = [];

  for (const [key, value] of entries) {
    const side = sideOfKey(key);
    const metric = key.slice(key.indexOf("-") + 1);
    if (side === null || metric === "") {
      rest.push([key, value]);
      continue;
    }

    const sides = sidesByMetric.get(metric) ?? { added: null, removed: null };
    if (sides[side] !== null) {
      rest.push([key, value]);
      continue;
    }
    sides[side] = value;
    sidesByMetric.set(metric, sides);
  }

  return {
    counts: [...sidesByMetric].map(([metric, sides]) => ({
      metric,
      added: countCell(sides.added, "added"),
      removed: countCell(sides.removed, "removed"),
    })),
    rest,
  };
};

export interface BeforeAfterRow {
  metric: string;
  before: string | null;
  after: string;
}

export const buildBeforeAfterRows = (
  totalEntries: SummaryEntry[],
  parentSummary: SnapshotSummary | null,
): BeforeAfterRow[] =>
  totalEntries.map(([key, value]) => ({
    metric: key.startsWith("total-") ? key.slice("total-".length) : key,
    before: parentSummary?.[key] ?? null,
    after: value,
  }));
