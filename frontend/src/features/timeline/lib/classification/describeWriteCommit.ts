import type {
  MetadataFileNode,
  SnapshotRefs,
  SnapshotSummary,
} from "../../api/nodeSchemas";
import { formatShortId } from "../format/formatShortId";
import {
  impactText,
  readRowCounts,
  readSnapshotImpact,
} from "../impactSegment";
import type { DescribedChange } from "../definitionChanges/describedChange";
import type { CommitDescription, SnapshotsById } from "./commitDescription";

const REWRITE_TITLES: Record<string, string> = {
  "rewrite data files": "Files compacted",
  "rewrite delete files": "Delete files compacted",
  "rewrite manifests": "Manifests rewritten",
  replace: "Files compacted",
};

const OPERATION_TITLES: Record<string, string> = {
  append: "Data added",
  delete: "Data deleted",
  overwrite: "Data overwritten",
};

/**
 * Engines name the same row change differently (an UPDATE is an `overwrite` in Spark, a `delete`
 * plus `append` elsewhere), so the title follows the row counts; `operation` only names rewrites.
 */
export const writeTitle = (
  operationDescription: string | null,
  summary: SnapshotSummary | null,
): string => {
  const rewriteTitle =
    operationDescription === null
      ? undefined
      : REWRITE_TITLES[operationDescription];
  if (rewriteTitle !== undefined) {
    return rewriteTitle;
  }

  const { addedRows, removedRows } =
    summary === null
      ? { addedRows: 0n, removedRows: 0n }
      : readRowCounts(summary);
  if (addedRows > 0n && removedRows > 0n) {
    return "Data overwritten";
  }
  if (addedRows > 0n) {
    return "Data added";
  }
  if (removedRows > 0n) {
    return "Data deleted";
  }

  const operationTitle =
    operationDescription === null
      ? undefined
      : OPERATION_TITLES[operationDescription];
  return operationTitle ?? "Data changed";
};

const findBranchNamePointingAt = (
  refs: SnapshotRefs,
  snapshotId: string,
): string | null => {
  const branchNames = Object.entries(refs)
    .filter(
      ([, ref]) => ref.type === "branch" && ref["snapshot-id"] === snapshotId,
    )
    .map(([name]) => name);
  if (branchNames.includes("main")) {
    return "main";
  }
  return branchNames[0] ?? null;
};

export const describeWriteCommit = (
  gainedSnapshotId: string,
  currentFile: MetadataFileNode,
  snapshotsById: SnapshotsById,
  definitionChanges: DescribedChange[],
): CommitDescription => {
  const snapshotRecord = snapshotsById.get(gainedSnapshotId);
  const snapshotImpact =
    snapshotRecord === undefined
      ? [impactText(`snapshot ${formatShortId(gainedSnapshotId)} unavailable`)]
      : readSnapshotImpact(snapshotRecord.summary, snapshotRecord.operation);
  const impactSegments = [
    ...snapshotImpact,
    ...definitionChanges.map((change) => impactText(change.impact)),
  ];
  const detailTexts = definitionChanges.map((change) => change.detail);

  const isNewCurrent = currentFile.snapshot_id === gainedSnapshotId;
  const pointingBranchName = findBranchNamePointingAt(
    currentFile.refs,
    gainedSnapshotId,
  );
  const isPublished = isNewCurrent || pointingBranchName !== null;

  if (!isPublished) {
    return {
      kind: "draft-write",
      title: "Data not published",
      impactSegments,
      detailTexts,
      snapshotId: gainedSnapshotId,
      branchName: null,
      repointTargetId: null,
      movedToBranchName: null,
    };
  }

  return {
    kind: "published-write",
    title: writeTitle(
      snapshotRecord?.operation_description ?? null,
      snapshotRecord?.summary ?? null,
    ),
    impactSegments,
    detailTexts,
    snapshotId: gainedSnapshotId,
    branchName: pointingBranchName,
    repointTargetId: null,
    movedToBranchName: null,
  };
};
