import type { SnapshotSummary } from "../../api/nodeSchemas";
import type { SummaryEntry } from "./groupSnapshotSummary";

export interface ChangeCountRow {
  metric: string;
  added: string | null;
  removed: string | null;
}

export interface PairedChangeCounts {
  paired: ChangeCountRow[];
  rest: SummaryEntry[];
}

const sideOfKey = (key: string): "added" | "removed" | null => {
  if (key.startsWith("added-")) {
    return "added";
  }
  if (key.startsWith("deleted-") || key.startsWith("removed-")) {
    return "removed";
  }
  return null;
};

// Iceberg spells the negative side either `deleted-` or `removed-`.
export const pairChangeCounts = (
  entries: SummaryEntry[],
): PairedChangeCounts => {
  const rowsByMetric = new Map<string, ChangeCountRow>();
  const rest: SummaryEntry[] = [];

  for (const [key, value] of entries) {
    const side = sideOfKey(key);
    const metric = key.slice(key.indexOf("-") + 1);
    if (side === null || metric === "") {
      rest.push([key, value]);
      continue;
    }

    const row = rowsByMetric.get(metric) ?? {
      metric,
      added: null,
      removed: null,
    };
    if (row[side] !== null) {
      rest.push([key, value]);
      continue;
    }
    row[side] = value;
    rowsByMetric.set(metric, row);
  }

  return { paired: [...rowsByMetric.values()], rest };
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
