import type { MetadataFileNode, SnapshotRefs } from "../../api/nodeSchemas";
import type { CommitDescription, SnapshotsById } from "./commitDescription";
import { formatShortId } from "../format/formatShortId";
import { formatDayMonthYearAndClock } from "../format/formatTimelineTime";
import { impactText } from "../impactSegment";
import type { DescribedChange } from "../definitionChanges/describedChange";

type AncestrySearchResult = "ancestor" | "not-ancestor" | "lineage-broken";

const searchAncestry = (
  candidateAncestorId: string,
  descendantId: string,
  snapshotsById: SnapshotsById,
): AncestrySearchResult => {
  const visitedIds = new Set<string>();
  let cursor = snapshotsById.get(descendantId);

  while (cursor !== undefined) {
    const parentId = cursor.parent_id;

    if (parentId === null) {
      return "not-ancestor";
    }
    if (parentId === candidateAncestorId) {
      return "ancestor";
    }
    if (visitedIds.has(parentId)) {
      return "not-ancestor";
    }

    visitedIds.add(parentId);
    cursor = snapshotsById.get(parentId);
  }

  return "lineage-broken";
};

/** Rollback is decided by ancestry alone; a broken lineage gets the neutral description. */
const isRolledBack = (
  previousCurrentId: string | null,
  targetId: string,
  snapshotsById: SnapshotsById,
): boolean =>
  previousCurrentId !== null &&
  searchAncestry(targetId, previousCurrentId, snapshotsById) === "ancestor";

const findBranchHeadName = (
  previousRefs: SnapshotRefs,
  targetId: string,
): string | null => {
  const branchHeadEntry = Object.entries(previousRefs).find(
    ([name, ref]) =>
      ref.type === "branch" &&
      name !== "main" &&
      ref["snapshot-id"] === targetId,
  );

  return branchHeadEntry?.[0] ?? null;
};

export const describeRePointCommit = (
  previousFile: MetadataFileNode,
  targetId: string,
  snapshotsById: SnapshotsById,
  definitionChanges: DescribedChange[],
): CommitDescription => {
  const targetRecord = snapshotsById.get(targetId);
  const rolledBack = isRolledBack(
    previousFile.snapshot_id,
    targetId,
    snapshotsById,
  );
  const movedToBranchName = rolledBack
    ? null
    : findBranchHeadName(previousFile.refs, targetId);
  const title = rolledBack
    ? "Rolled back"
    : movedToBranchName !== null
      ? `main moved to ${movedToBranchName}`
      : "Switched snapshots";
  const targetImpact =
    targetRecord === undefined
      ? `→ snapshot ${formatShortId(targetId)} (expired or not loaded)`
      : `→ snapshot ${formatShortId(targetId)} · ${formatDayMonthYearAndClock(targetRecord.timestamp)}`;

  return {
    kind: "re-point",
    title,
    impactSegments: [
      impactText(targetImpact),
      ...definitionChanges.map((change) => impactText(change.impact)),
    ],
    detailTexts: definitionChanges.map((change) => change.detail),
    snapshotId: null,
    branchName: null,
    repointTargetId: targetId,
    movedToBranchName,
  };
};
