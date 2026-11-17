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
  detailTexts: [],
  snapshotId: null,
  branchName: null,
  repointTargetId: null,
  movedToBranchName: null,
};

const buildOldestRow = (
  file: MetadataFileNode,
  snapshotsById: SnapshotsById,
): TimelineRow => {
  const ownSnapshot =
    file.snapshot_id === null ? undefined : snapshotsById.get(file.snapshot_id);
  // With older commits cut off, the snapshot may have been written before the
  // loaded range, so the oldest file cannot be shown as a write.
  const isHistoryCutOff = (file.pointed_metadata_log_count ?? 0) > 0;

  if (ownSnapshot === undefined || isHistoryCutOff) {
    const hasUnknowableSnapshot = file.snapshot_id !== null;
    if (hasUnknowableSnapshot || isHistoryCutOff) {
      return boundaryRow(file);
    }
    return {
      ...boundaryRow(file),
      kind: "metadata-only",
      title: "Start of history",
    };
  }

  return {
    ...boundaryRow(file),
    kind: "published-write",
    title: writeTitle(ownSnapshot.operation_description, ownSnapshot.summary),
    impact: readSnapshotImpact(ownSnapshot.summary, ownSnapshot.operation),
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
  details: commit.detailTexts,
  repointTargetId: commit.repointTargetId,
  shortId: formatShortId(commit.snapshotId),
  snapshotId: commit.snapshotId,
  branchName: commit.branchName,
  movedToBranchName: commit.movedToBranchName,
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
  tableMetadata: TableMetadata | null,
): TimelineData => {
  const { metadataFiles, snapshotsById, unreadableCommitCount } =
    parseTimelineNodes(nodes);
  const filesOldestFirst = [...metadataFiles].sort(
    (fileA, fileB) =>
      fileA.timestamp - fileB.timestamp ||
      fileA.file_path.localeCompare(fileB.file_path),
  );
  const newestRefs = filesOldestFirst.at(-1)?.refs ?? {};

  const rowsOldestFirst = filesOldestFirst.map((file, index): TimelineRow => {
    const previousFile = filesOldestFirst[index - 1];

    if (previousFile === undefined) {
      return attachRefBadges(buildOldestRow(file, snapshotsById), newestRefs);
    }

    let commit = DEGRADED_COMMIT;
    try {
      commit = describeCommit(
        previousFile,
        file,
        snapshotsById,
        tableMetadata ?? {},
      );
    } catch {
      // the row still renders, titled by the fallback
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
    unreadableCommitCount,
    isTableMetadataUnreadable: tableMetadata === null,
    // previous_file is nulled at the load cutoff; the metadata log count survives it
    olderCommitCount: filesOldestFirst[0]?.pointed_metadata_log_count ?? 0,
    snapshotsById,
    filesByPath: new Map(
      filesOldestFirst.map((file) => [file.file_path, file]),
    ),
  };
};
