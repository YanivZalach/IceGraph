import type {
  MetadataFileNode,
  SnapshotRefs,
  SnapshotSummary,
} from "../../api/nodeSchemas";
import {
  impactText,
  isProvablyUnchangedWrite,
  readRowCounts,
  readSnapshotImpact,
} from "../impactSegment";
import type { DescribedChange } from "../definitionChanges/describedChange";
import type {
  CommitDescription,
  Headline,
  SnapshotsById,
} from "./commitDescription";

const DATA_ADDED: Headline = { title: "Data added", category: "data-added" };
const DATA_DELETED: Headline = {
  title: "Data deleted",
  category: "data-removed",
};
const DATA_OVERWRITTEN: Headline = {
  title: "Data overwritten",
  category: "data-replaced",
};

const maintenance = (title: string): Headline => ({
  title,
  category: "maintenance",
});

const REWRITE_HEADLINES: Record<string, Headline> = {
  "rewrite data files": maintenance("Files compacted"),
  "rewrite delete files": maintenance("Delete files compacted"),
  "rewrite manifests": maintenance("Manifests rewritten"),
  replace: maintenance("Files compacted"),
};

const OPERATION_HEADLINES: Record<string, Headline> = {
  append: DATA_ADDED,
  delete: DATA_DELETED,
  overwrite: DATA_OVERWRITTEN,
};

/**
 * Engines name the same row change differently (an UPDATE is an `overwrite` in Spark, a `delete`
 * plus `append` elsewhere), so the title follows the row counts; `operation` only names rewrites.
 */
export const writeHeadline = (
  operationDescription: string | null,
  summary: SnapshotSummary | null,
): Headline => {
  const rewriteHeadline =
    operationDescription === null
      ? undefined
      : REWRITE_HEADLINES[operationDescription];
  if (rewriteHeadline !== undefined) {
    return rewriteHeadline;
  }

  const { addedRows, removedRows } =
    summary === null
      ? { addedRows: 0n, removedRows: 0n }
      : readRowCounts(summary);
  if (addedRows > 0n && removedRows > 0n) {
    return DATA_OVERWRITTEN;
  }
  if (addedRows > 0n) {
    return DATA_ADDED;
  }
  if (removedRows > 0n) {
    return DATA_DELETED;
  }
  if (summary !== null && isProvablyUnchangedWrite(summary)) {
    return { title: "No data changed", category: "quiet" };
  }

  const operationHeadline =
    operationDescription === null
      ? undefined
      : OPERATION_HEADLINES[operationDescription];
  return operationHeadline ?? { title: "Data changed", category: "quiet" };
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
      ? [impactText("row counts unknown: snapshot expired or not loaded")]
      : readSnapshotImpact(snapshotRecord.summary, snapshotRecord.operation);
  const impactSegments = [
    ...snapshotImpact,
    ...definitionChanges.map(
      (change) => change.impactSegment ?? impactText(change.impact),
    ),
  ];
  const detailTexts = definitionChanges.map((change) => change.detail);

  const isNewCurrent = currentFile.snapshot_id === gainedSnapshotId;
  const pointingBranchName = findBranchNamePointingAt(
    currentFile.refs,
    gainedSnapshotId,
  );
  const isReferenced = Object.values(currentFile.refs).some(
    (ref) => ref["snapshot-id"] === gainedSnapshotId,
  );
  const isPublished = isNewCurrent || isReferenced;

  if (!isPublished) {
    return {
      kind: "draft-write",
      title: "Data not published",
      category: "unpublished",
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
    ...writeHeadline(
      snapshotRecord?.operation_description ?? null,
      snapshotRecord?.summary ?? null,
    ),
    impactSegments,
    detailTexts,
    snapshotId: gainedSnapshotId,
    // the current snapshot is main's head, also on v1 tables that have no refs
    branchName: pointingBranchName ?? (isNewCurrent ? "main" : null),
    repointTargetId: null,
    movedToBranchName: null,
  };
};
