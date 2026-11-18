import type { MetadataFileNode, SnapshotRefs } from "../api/nodeSchemas";
import type { TableMetadata } from "../api/tableMetadataSchema";
import { readSnapshotImpact } from "./impactSegment";
import type {
  CommitDescription,
  SnapshotsById,
} from "./classification/commitDescription";
import { describeCommit } from "./classification/describeCommit";
import { writeHeadline } from "./classification/describeWriteCommit";
import { findDraftPublication } from "./findDraftPublication";
import { formatShortId } from "./format/formatShortId";
import { parseTimelineNodes } from "./parseTimelineNodes";
import {
  sequenceTimelineFiles,
  type SequenceItem,
  type UnreadableRun,
} from "./sequenceTimelineFiles";
import {
  boundaryRow,
  type CommitRow,
  type GapRow,
  type RefBadge,
  type TimelineData,
  type TimelineRow,
} from "./timelineRow";

const DEGRADED_COMMIT: CommitDescription = {
  kind: "metadata-only",
  title: "Metadata updated",
  category: "quiet",
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
): CommitRow => {
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
      category: "quiet",
    };
  }

  return {
    ...boundaryRow(file),
    kind: "published-write",
    branchName: "main",
    ...writeHeadline(ownSnapshot.operation_description, ownSnapshot.summary),
    impact: readSnapshotImpact(ownSnapshot.summary, ownSnapshot.operation),
    shortId: formatShortId(ownSnapshot.snapshot_id),
    snapshotId: ownSnapshot.snapshot_id,
  };
};

const toTimelineRow = (
  file: MetadataFileNode,
  commit: CommitDescription,
): CommitRow => ({
  ...boundaryRow(file),
  kind: commit.kind,
  title: commit.title,
  category: commit.category,
  impact: commit.impactSegments,
  details: commit.detailTexts,
  repointTargetId: commit.repointTargetId,
  shortId: formatShortId(commit.snapshotId),
  snapshotId: commit.snapshotId,
  branchName: commit.branchName,
  movedToBranchName: commit.movedToBranchName,
});

const attachRefBadges = (
  row: CommitRow,
  newestRefs: SnapshotRefs,
): CommitRow => {
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
  row: CommitRow,
  laterFiles: MetadataFileNode[],
  snapshotsById: SnapshotsById,
): CommitRow => {
  if (row.kind !== "draft-write" || row.snapshotId === null) {
    return row;
  }

  return {
    ...row,
    ...findDraftPublication(row.snapshotId, laterFiles, snapshotsById),
  };
};

const toGapRow = (files: UnreadableRun): GapRow => ({
  kind: "gap",
  filePath: files[0].file_path,
  timestampMs: files[0].timestamp,
  unreadableFiles: files.map((file) => ({
    path: file.file_path,
    timestampMs: file.timestamp,
    reasons: file.reasons,
  })),
});

interface DescribeContext {
  snapshotsById: SnapshotsById;
  tableMetadata: TableMetadata | null;
  newestRefs: SnapshotRefs;
}

const toCommitRow = (
  { file, predecessor }: Extract<SequenceItem, { kind: "commit" }>,
  laterFiles: MetadataFileNode[],
  { snapshotsById, tableMetadata, newestRefs }: DescribeContext,
): CommitRow => {
  if (predecessor.kind === "none") {
    return attachRefBadges(buildOldestRow(file, snapshotsById), newestRefs);
  }
  if (predecessor.kind === "unreadable") {
    return {
      ...boundaryRow(file),
      kind: "unknown-change",
      title: "Change unknown",
      category: "quiet",
    };
  }

  let commit = DEGRADED_COMMIT;
  try {
    commit = describeCommit(
      predecessor.file,
      file,
      snapshotsById,
      tableMetadata ?? {},
    );
  } catch {
    // the row still renders, titled by the fallback
  }

  return attachDraftPublication(
    attachRefBadges(toTimelineRow(file, commit), newestRefs),
    laterFiles,
    snapshotsById,
  );
};

/** Parse, sequence (which orders), then turn each sequenced item into a row. */
export const buildTimeline = (
  nodes: unknown[],
  tableMetadata: TableMetadata | null,
): TimelineData => {
  const {
    readableFiles,
    unreadableFiles,
    snapshotsById,
    unreadableCommitCount,
  } = parseTimelineNodes(nodes);
  const sequence = sequenceTimelineFiles(readableFiles, unreadableFiles);
  const filesOldestFirst = sequence.flatMap((item) =>
    item.kind === "commit" ? [item.file] : [],
  );
  const context: DescribeContext = {
    snapshotsById,
    tableMetadata,
    newestRefs: filesOldestFirst.at(-1)?.refs ?? {},
  };

  const rowsOldestFirst: TimelineRow[] = [];
  let describedFileCount = 0;
  for (const item of sequence) {
    if (item.kind === "gap") {
      rowsOldestFirst.push(toGapRow(item.files));
      continue;
    }
    describedFileCount += 1;
    rowsOldestFirst.push(
      toCommitRow(item, filesOldestFirst.slice(describedFileCount), context),
    );
  }

  const oldestItem = sequence[0];
  const loadedButUnreadableOlderCount =
    oldestItem?.kind === "gap" ? oldestItem.files.length : 0;

  return {
    rows: rowsOldestFirst.reverse(),
    unreadableCommitCount,
    isTableMetadataUnreadable: tableMetadata === null,
    // previous_file is nulled at the load cutoff; the metadata log count survives it
    olderCommitCount: Math.max(
      0,
      (filesOldestFirst[0]?.pointed_metadata_log_count ?? 0) -
        loadedButUnreadableOlderCount,
    ),
    snapshotsById,
    filesByPath: new Map(
      filesOldestFirst.map((file) => [file.file_path, file]),
    ),
  };
};
