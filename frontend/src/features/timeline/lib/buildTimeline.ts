// Read docs/agents/frontend/timeline.md before changing this file or anything it calls.
import type { MetadataFileNode, SnapshotRefs } from "../api/nodeSchemas";
import type { TableMetadata } from "../api/tableMetadataSchema";
import { buildBranchLanes } from "./buildBranchLanes";
import type {
  CommitDescription,
  SnapshotsById,
} from "./classification/commitDescription";
import { describeCommit } from "./classification/describeCommit";
import { UNRECOGNIZED_METADATA_TITLE } from "./classification/describeMetadataOnlyCommit";
import { describeWriteCommit } from "./classification/describeWriteCommit";
import { findDraftPublication } from "./findDraftPublication";
import { formatShortId } from "./format/formatShortId";
import { parseTimelineNodes } from "./parseTimelineNodes";
import {
  sequenceTimelineFiles,
  type SequenceItem,
  type UnreadableRun,
} from "./sequenceTimelineFiles";
import {
  commitRowDefaults,
  type CommitRow,
  type GapRow,
  type RefBadge,
  type TimelineData,
  type TimelineRow,
} from "./timelineRow";

const DEGRADED_COMMIT: CommitDescription = {
  kind: "metadata-only",
  title: UNRECOGNIZED_METADATA_TITLE,
  category: "quiet",
  impactSegments: [],
  detailTexts: [],
  snapshotId: null,
  branchName: null,
  repointTargetId: null,
  movedToBranchName: null,
};

const toTimelineRow = (
  file: MetadataFileNode,
  commit: CommitDescription,
): CommitRow => ({
  ...commitRowDefaults(file),
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

const toUnknownChangeRow = (file: MetadataFileNode): CommitRow => ({
  ...commitRowDefaults(file),
  kind: "unknown-change",
  title: "Change unknown",
  category: "quiet",
});

const findSnapshotWrittenBy = (
  file: MetadataFileNode,
  snapshotsById: SnapshotsById,
): string | null => {
  for (const pointedSnapshot of file.pointed_snapshots_files ?? []) {
    const snapshot = snapshotsById.get(pointedSnapshot["snapshot-id"]);
    if (snapshot !== undefined && snapshot.timestamp === file.timestamp) {
      return snapshot.snapshot_id;
    }
  }
  return null;
};

const buildOldestRow = (
  file: MetadataFileNode,
  snapshotsById: SnapshotsById,
): CommitRow => {
  const isHistoryCutOff = (file.pointed_metadata_log_count ?? 0) > 0;
  const loadedOwnSnapshotId =
    file.snapshot_id !== null && snapshotsById.has(file.snapshot_id)
      ? file.snapshot_id
      : null;
  const writtenSnapshotId = isHistoryCutOff
    ? findSnapshotWrittenBy(file, snapshotsById)
    : loadedOwnSnapshotId;

  if (writtenSnapshotId !== null) {
    return toTimelineRow(
      file,
      describeWriteCommit(writtenSnapshotId, file, snapshotsById, []),
    );
  }
  if (isHistoryCutOff || file.snapshot_id !== null) {
    return toUnknownChangeRow(file);
  }
  return {
    ...commitRowDefaults(file),
    kind: "metadata-only",
    title: "Start of history",
    category: "quiet",
  };
};

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
    return attachDraftPublication(
      attachRefBadges(buildOldestRow(file, snapshotsById), newestRefs),
      laterFiles,
      snapshotsById,
    );
  }
  if (predecessor.kind === "unreadable") {
    return toUnknownChangeRow(file);
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

  return {
    ...attachDraftPublication(
      attachRefBadges(toTimelineRow(file, commit), newestRefs),
      laterFiles,
      snapshotsById,
    ),
    previousFilePath: predecessor.file.file_path,
  };
};

const readableFilesUntilNextGap = (
  laterItems: SequenceItem[],
): MetadataFileNode[] => {
  const files: MetadataFileNode[] = [];
  for (const item of laterItems) {
    if (item.kind === "gap") {
      break;
    }
    files.push(item.file);
  }
  return files;
};

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
  const newestItem = sequence.at(-1);
  const context: DescribeContext = {
    snapshotsById,
    tableMetadata,
    // Unreadable newer events may have moved the refs, so no row is badged as their target.
    newestRefs: newestItem?.kind === "commit" ? newestItem.file.refs : {},
  };

  const rowsOldestFirst = sequence.map((item, index): TimelineRow =>
    item.kind === "gap"
      ? toGapRow(item.files)
      : toCommitRow(
          item,
          readableFilesUntilNextGap(sequence.slice(index + 1)),
          context,
        ),
  );

  const branchGraph = buildBranchLanes(
    sequence,
    rowsOldestFirst,
    snapshotsById,
  );
  const oldestItem = sequence[0];
  const loadedButUnreadableOlderCount =
    oldestItem?.kind === "gap" ? oldestItem.files.length : 0;

  return {
    rows: rowsOldestFirst.reverse(),
    unreadableCommitCount,
    isTableMetadataUnreadable: tableMetadata === null,
    // previous_file is nulled at the load cutoff; the metadata log count survives it
    hasOlderEvents:
      (filesOldestFirst[0]?.pointed_metadata_log_count ?? 0) >
      loadedButUnreadableOlderCount,
    snapshotsById,
    filesByPath: new Map(
      filesOldestFirst.map((file) => [file.file_path, file]),
    ),
    branchGraph,
  };
};
