import type { MetadataFileNode } from "../../api/nodeSchemas";
import type { TableMetadata } from "../../api/tableMetadataSchema";
import type { CommitDescription, SnapshotsById } from "./commitDescription";
import { listDefinitionChanges } from "../definitionChanges/listDefinitionChanges";
import { impactText } from "../impactSegment";
import { describeMetadataOnlyCommit } from "./describeMetadataOnlyCommit";
import { describeRePointCommit } from "./describeRePointCommit";
import { describeWriteCommit } from "./describeWriteCommit";
import {
  countExpiredSnapshots,
  pickGainedSnapshotId,
} from "./snapshotListDiff";

export const describeCommit = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
  snapshotsById: SnapshotsById,
  tableMetadata: TableMetadata,
): CommitDescription => {
  const gainedSnapshotId = pickGainedSnapshotId(previousFile, currentFile);
  const newCurrentId = currentFile.snapshot_id;
  const rePointTargetId =
    gainedSnapshotId === null &&
    newCurrentId !== null &&
    newCurrentId !== previousFile.snapshot_id
      ? newCurrentId
      : null;

  const definitionImpacts = listDefinitionChanges(
    previousFile,
    currentFile,
    tableMetadata,
    gainedSnapshotId ?? rePointTargetId,
  ).map((change) => impactText(change.impact));

  if (gainedSnapshotId !== null) {
    return describeWriteCommit(
      gainedSnapshotId,
      currentFile,
      snapshotsById,
      definitionImpacts,
    );
  }

  if (rePointTargetId !== null) {
    return describeRePointCommit(
      previousFile,
      rePointTargetId,
      snapshotsById,
      definitionImpacts,
    );
  }

  return describeMetadataOnlyCommit(
    previousFile,
    currentFile,
    tableMetadata,
    countExpiredSnapshots(previousFile, currentFile),
  );
};
