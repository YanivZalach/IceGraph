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
import { orderMetadataFilesOldestFirst } from "./orderMetadataFiles";
import { parseTimelineNodes } from "./parseTimelineNodes";
import {
  boundaryRow,
  type RefBadge,
  type TimelineData,
  type TimelineRow,
  type UnreadableFileSummary,
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
): TimelineRow => ({
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
  const {
    metadataFiles,
    snapshotsById,
    unreadableCommitCount,
    unreadableFiles,
  } = parseTimelineNodes(nodes);
  const readableByPath = new Map(
    metadataFiles.map((file) => [file.file_path, file]),
  );
  const filesOldestFirst: MetadataFileNode[] = [];
  const unreadableBefore: UnreadableFileSummary[][] = [];
  let pendingUnreadable: UnreadableFileSummary[] = [];
  for (const placedFile of orderMetadataFilesOldestFirst([
    ...metadataFiles,
    ...unreadableFiles,
  ])) {
    const readableFile = readableByPath.get(placedFile.file_path);
    if (readableFile === undefined) {
      pendingUnreadable.push({
        path: placedFile.file_path,
        reasons: "reasons" in placedFile ? placedFile.reasons : [],
      });
      continue;
    }
    filesOldestFirst.push(readableFile);
    unreadableBefore.push(pendingUnreadable);
    pendingUnreadable = [];
  }
  const newestRefs = filesOldestFirst.at(-1)?.refs ?? {};

  const rowsOldestFirst = filesOldestFirst.map((file, index): TimelineRow => {
    const previousFile = filesOldestFirst[index - 1];

    if (previousFile === undefined) {
      return {
        ...attachRefBadges(buildOldestRow(file, snapshotsById), newestRefs),
        unreadableCommitsBefore: unreadableBefore[index]?.length ?? 0,
        unreadableFilesBefore: unreadableBefore[index] ?? [],
      };
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

    const isPreviousFileMissing =
      file.previous_file !== null && !readableByPath.has(file.previous_file);
    const row = {
      ...attachRefBadges(toTimelineRow(file, commit), newestRefs),
      unreadableCommitsBefore: Math.max(
        unreadableBefore[index]?.length ?? 0,
        isPreviousFileMissing ? 1 : 0,
      ),
      unreadableFilesBefore: unreadableBefore[index] ?? [],
    };

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
    olderCommitCount: Math.max(
      0,
      (filesOldestFirst[0]?.pointed_metadata_log_count ?? 0) -
        (unreadableBefore[0]?.length ?? 0),
    ),
    snapshotsById,
    filesByPath: new Map(
      filesOldestFirst.map((file) => [file.file_path, file]),
    ),
  };
};
