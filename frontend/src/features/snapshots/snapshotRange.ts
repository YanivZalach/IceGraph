import type { SnapshotMap } from "./api/snapshotQueries";

export interface SnapshotEntry {
  snapshotId: string;
  timestamp: string;
  operation: string;
}

// An empty start means full history and an empty end means latest, matching
// the omitted start_snapshot_id and end_snapshot_id URL parameters.
export interface SnapshotRange {
  startSnapshotId: string;
  endSnapshotId: string;
  anchorSnapshotId: string | null;
}

const DEFAULT_RANGE_LENGTH = 3;

export const sortSnapshotEntries = (snapshots: SnapshotMap): SnapshotEntry[] =>
  Object.entries(snapshots)
    .sort(([first], [second]) => second.localeCompare(first))
    .map(([timestamp, { snapshot_id, operation }]) => ({
      snapshotId: snapshot_id,
      timestamp,
      operation,
    }));

export const defaultSnapshotRange = (
  entries: readonly SnapshotEntry[],
): SnapshotRange => ({
  startSnapshotId:
    entries[Math.min(entries.length, DEFAULT_RANGE_LENGTH) - 1]?.snapshotId ??
    "",
  endSnapshotId: "",
  anchorSnapshotId: null,
});

const entryIndex = (
  entries: readonly SnapshotEntry[],
  snapshotId: string,
): number => entries.findIndex((entry) => entry.snapshotId === snapshotId);

export const selectSnapshot = (
  range: SnapshotRange,
  entries: readonly SnapshotEntry[],
  snapshotId: string,
): SnapshotRange => {
  const anchor = range.anchorSnapshotId;
  if (anchor === null)
    return {
      startSnapshotId: snapshotId,
      endSnapshotId: snapshotId,
      anchorSnapshotId: snapshotId,
    };
  const isAnchorNewer =
    entryIndex(entries, anchor) <= entryIndex(entries, snapshotId);
  return {
    startSnapshotId: isAnchorNewer ? snapshotId : anchor,
    endSnapshotId: isAnchorNewer ? anchor : snapshotId,
    anchorSnapshotId: null,
  };
};

export const selectLatestEnd = (range: SnapshotRange): SnapshotRange => ({
  startSnapshotId: range.anchorSnapshotId ?? range.startSnapshotId,
  endSnapshotId: "",
  anchorSnapshotId: null,
});

export const selectFullHistoryStart = (
  range: SnapshotRange,
): SnapshotRange => ({
  startSnapshotId: "",
  endSnapshotId: range.anchorSnapshotId ?? range.endSnapshotId,
  anchorSnapshotId: null,
});

export const isEntryInRange = (
  range: SnapshotRange,
  entries: readonly SnapshotEntry[],
  index: number,
): boolean => {
  const startIndex =
    range.startSnapshotId === ""
      ? entries.length
      : entryIndex(entries, range.startSnapshotId);
  const endIndex =
    range.endSnapshotId === "" ? -1 : entryIndex(entries, range.endSnapshotId);
  return endIndex <= index && index <= startIndex;
};
