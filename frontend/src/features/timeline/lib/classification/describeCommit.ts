import type { MetadataFileNode } from "../../api/nodeSchemas";
import type { TableMetadata } from "../../api/tableMetadataSchema";
import type { CommitDescription, SnapshotsById } from "./commitDescription";
import { listDefinitionChanges } from "../definitionChanges/listDefinitionChanges";
import { describeMetadataOnlyCommit } from "./describeMetadataOnlyCommit";
import { describeRePointCommit } from "./describeRePointCommit";
import { describeWriteCommit } from "./describeWriteCommit";
import { pickGainedSnapshotId } from "./pickGainedSnapshotId";

export const describeCommit = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
  snapshotsById: SnapshotsById,
  tableMetadata: TableMetadata,
): CommitDescription => {
  const definitionImpacts = listDefinitionChanges(
    previousFile,
    currentFile,
    tableMetadata,
  ).map((change) => change.impact);

  const gainedSnapshotId = pickGainedSnapshotId(previousFile, currentFile);
  if (gainedSnapshotId !== null) {
    return describeWriteCommit(
      gainedSnapshotId,
      currentFile,
      snapshotsById,
      definitionImpacts,
    );
  }

  const newCurrentId = currentFile.snapshot_id;
  if (newCurrentId !== null && newCurrentId !== previousFile.snapshot_id) {
    return describeRePointCommit(
      previousFile,
      newCurrentId,
      snapshotsById,
      definitionImpacts,
    );
  }

  return describeMetadataOnlyCommit(previousFile, currentFile, tableMetadata);
};
