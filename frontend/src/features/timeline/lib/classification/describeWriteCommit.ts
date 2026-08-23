import type { MetadataFileNode, SnapshotRefs } from "../../api/nodeSchemas";
import { formatShortId } from "../format/formatShortId";
import {
  impactText,
  readSnapshotImpact,
  type ImpactSegment,
} from "../impactSegment";
import type { CommitDescription, SnapshotsById } from "./commitDescription";

const WRITE_TITLES: Record<string, string> = {
  append: "Data added",
  delete: "Data deleted",
  overwrite: "Data rewritten",
  "rewrite data files": "Files compacted",
  "rewrite delete files": "Delete files compacted",
  "rewrite manifests": "Manifests rewritten",
  replace: "Files compacted",
};

export const writeTitle = (operationDescription: string | null): string => {
  if (operationDescription === null) {
    return "Data changed";
  }
  return WRITE_TITLES[operationDescription] ?? "Data changed";
};

const findRefNamePointingAt = (
  refs: SnapshotRefs,
  snapshotId: string,
): string | null => {
  const pointingEntry = Object.entries(refs).find(
    ([, ref]) => ref["snapshot-id"] === snapshotId,
  );
  return pointingEntry === undefined ? null : pointingEntry[0];
};

export const describeWriteCommit = (
  gainedSnapshotId: string,
  currentFile: MetadataFileNode,
  snapshotsById: SnapshotsById,
  definitionImpacts: ImpactSegment[],
): CommitDescription => {
  const snapshotRecord = snapshotsById.get(gainedSnapshotId);
  const snapshotImpact =
    snapshotRecord === undefined
      ? [impactText(`snapshot ${formatShortId(gainedSnapshotId)} unavailable`)]
      : readSnapshotImpact(snapshotRecord.summary);
  const impactSegments = [...snapshotImpact, ...definitionImpacts];

  const isNewCurrent = currentFile.snapshot_id === gainedSnapshotId;
  const pointingBranchName = findRefNamePointingAt(
    currentFile.refs,
    gainedSnapshotId,
  );
  const isPublished = isNewCurrent || pointingBranchName !== null;

  if (!isPublished) {
    return {
      kind: "draft-write",
      title: "Data not published",
      impactSegments,
      snapshotId: gainedSnapshotId,
      branchName: null,
      repointTargetId: null,
    };
  }

  return {
    kind: "published-write",
    title: writeTitle(snapshotRecord?.operation_description ?? null),
    impactSegments,
    snapshotId: gainedSnapshotId,
    branchName: pointingBranchName,
    repointTargetId: null,
  };
};
