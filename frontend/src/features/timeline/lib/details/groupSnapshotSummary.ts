import type { SnapshotSummary } from "../../api/nodeSchemas";

export type SummaryEntry = [key: string, value: string];

export interface SummaryGroups {
  thisChange: SummaryEntry[];
  tableAfter: SummaryEntry[];
  engine: SummaryEntry[];
}

const ENGINE_KEYS = new Set(["app-id", "iceberg-version"]);

const isTableAfter = (key: string): boolean => key.startsWith("total-");

const isEngine = (key: string): boolean =>
  ENGINE_KEYS.has(key) || key.startsWith("engine-") || key.startsWith("spark.");

export const groupSnapshotSummary = (
  summary: SnapshotSummary,
): SummaryGroups => {
  const groups: SummaryGroups = {
    thisChange: [],
    tableAfter: [],
    engine: [],
  };

  for (const [key, value] of Object.entries(summary)) {
    if (isTableAfter(key)) {
      groups.tableAfter.push([key, value]);
    } else if (isEngine(key)) {
      groups.engine.push([key, value]);
    } else {
      groups.thisChange.push([key, value]);
    }
  }

  return groups;
};
