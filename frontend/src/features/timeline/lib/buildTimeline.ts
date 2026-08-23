import type { MetadataFileNode, SnapshotRefs } from "../api/nodeSchemas";
import type { TableMetadata } from "../api/tableMetadataSchema";
import { readSnapshotImpact } from "./impactSegment";
import type {
  CommitDescription,
  SnapshotsById,
} from "./classification/commitDescription";
import { describeCommit } from "./classification/describeCommit";
import { writeTitle } from "./classification/describeWriteCommit";
import { findDraftPublication } from "./findDraftPublication";
import { formatShortId } from "./format/formatShortId";
import { parseTimelineNodes } from "./parseTimelineNodes";
import {
  boundaryRow,
  type RefBadge,
  type TimelineData,
  type TimelineRow,
} from "./timelineRow";

const DEGRADED_COMMIT: CommitDescription = {
  kind: "metadata-only",
  title: "Metadata updated",
  impactSegments: [],
  snapshotId: null,
  branchName: null,
  repointTargetId: null,
};

const buildOldestRow = (
  file: MetadataFileNode,
  snapshotsById: SnapshotsById,
): TimelineRow => {
  const ownSnapshot =
    file.snapshot_id === null ? undefined : snapshotsById.get(file.snapshot_id);

  if (ownSnapshot === undefined) {
    return boundaryRow(file);
  }

  return {
    ...boundaryRow(file),
    kind: "published-write",
    title: writeTitle(ownSnapshot.operation_description),
    impact: readSnapshotImpact(ownSnapshot.summary),
    shortId: formatShortId(ownSnapshot.snapshot_id),
    snapshotId: ownSnapshot.snapshot_id,
  };
};

const toTimelineRow = (
  file: MetadataFileNode,
  commit: CommitDescription,
): TimelineRow => ({
  ...boundaryRow(file),
  kind: commit.kind,
  title: commit.title,
  impact: commit.impactSegments,
  shortId: formatShortId(commit.snapshotId),
  snapshotId: commit.snapshotId,
  branchName: commit.branchName,
});

const attachRefBadges = (
  row: TimelineRow,
  newestRefs: SnapshotRefs,
): TimelineRow => {
  if (row.snapshotId === null) {
    return row;
  }

  const badges: RefBadge[] = [];
  for (const [name, ref] of Object.entries(newestRefs)) {
    if (ref["snapshot-id"] === row.snapshotId) {
      badges.push({ name, type: ref.type });
    }
  }

  return { ...row, badges };
};

const attachDraftPublication = (
  row: TimelineRow,
  laterFiles: MetadataFileNode[],
  snapshotsById: SnapshotsById,
): TimelineRow => {
  if (row.kind !== "draft-write" || row.snapshotId === null) {
    return row;
  }

  return {
    ...row,
    ...findDraftPublication(row.snapshotId, laterFiles, snapshotsById),
  };
};

export const buildTimeline = (
  nodes: unknown[],
  tableMetadata: TableMetadata,
): TimelineData => {
  const { metadataFiles, snapshotsById, skippedNodeCount } =
    parseTimelineNodes(nodes);
  const filesOldestFirst = [...metadataFiles].sort(
    (fileA, fileB) => fileA.timestamp - fileB.timestamp,
  );
  const newestRefs = filesOldestFirst.at(-1)?.refs ?? {};
  let degradedRowCount = 0;

  const rowsOldestFirst = filesOldestFirst.map((file, index): TimelineRow => {
    const previousFile = filesOldestFirst[index - 1];

    if (previousFile === undefined) {
      return attachRefBadges(buildOldestRow(file, snapshotsById), newestRefs);
    }

    let commit = DEGRADED_COMMIT;
    try {
      commit = describeCommit(previousFile, file, snapshotsById, tableMetadata);
    } catch {
      degradedRowCount += 1;
    }

    const row = attachRefBadges(toTimelineRow(file, commit), newestRefs);

    return attachDraftPublication(
      row,
      filesOldestFirst.slice(index + 1),
      snapshotsById,
    );
  });

  const rowsNewestFirst = rowsOldestFirst.reverse();

  return {
    rows: rowsNewestFirst,
    skippedNodeCount: skippedNodeCount + degradedRowCount,
  };
};
