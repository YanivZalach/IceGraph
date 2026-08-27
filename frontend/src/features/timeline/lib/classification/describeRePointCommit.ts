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

/** Rollback is decided by ancestry, not age; timestamps only when expired parents break the lineage. */
const isRolledBack = (
  previousCurrentId: string | null,
  targetId: string,
  snapshotsById: SnapshotsById,
): boolean => {
  if (previousCurrentId === null) {
    return false;
  }

  const ancestry = searchAncestry(targetId, previousCurrentId, snapshotsById);
  if (ancestry === "ancestor") {
    return true;
  }
  if (ancestry === "not-ancestor") {
    return false;
  }

  const targetRecord = snapshotsById.get(targetId);
  const previousRecord = snapshotsById.get(previousCurrentId);

  return (
    targetRecord !== undefined &&
    previousRecord !== undefined &&
    targetRecord.timestamp < previousRecord.timestamp
  );
};

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

const rePointTitle = (
  previousFile: MetadataFileNode,
  targetId: string,
  snapshotsById: SnapshotsById,
): string => {
  if (isRolledBack(previousFile.snapshot_id, targetId, snapshotsById)) {
    return "Rolled back";
  }

  const branchHeadName = findBranchHeadName(previousFile.refs, targetId);
  if (branchHeadName !== null) {
    return `main moved to ${branchHeadName}`;
  }

  return "Switched snapshots";
};

export const describeRePointCommit = (
  previousFile: MetadataFileNode,
  targetId: string,
  snapshotsById: SnapshotsById,
  definitionChanges: DescribedChange[],
): CommitDescription => {
  const targetRecord = snapshotsById.get(targetId);
  const targetImpact =
    targetRecord === undefined
      ? `→ snapshot ${formatShortId(targetId)} (expired or not loaded)`
      : `→ snapshot ${formatShortId(targetId)} · ${formatDayMonthYearAndClock(targetRecord.timestamp)}`;

  return {
    kind: "re-point",
    title: rePointTitle(previousFile, targetId, snapshotsById),
    impactSegments: [
      impactText(targetImpact),
      ...definitionChanges.map((change) => impactText(change.impact)),
    ],
    detailTexts: definitionChanges.map((change) => change.detail),
    snapshotId: null,
    branchName: null,
    repointTargetId: targetId,
  };
};
