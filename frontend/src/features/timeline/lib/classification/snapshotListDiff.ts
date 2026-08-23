import type { MetadataFileNode } from "../../api/nodeSchemas";

const listSnapshotIds = (file: MetadataFileNode): string[] =>
  (file.pointed_snapshots_files ?? []).map(
    (snapshot) => snapshot["snapshot-id"],
  );

const isPointedAtByAnyRef = (
  file: MetadataFileNode,
  snapshotId: string,
): boolean =>
  Object.values(file.refs).some((ref) => ref["snapshot-id"] === snapshotId);

/**
 * One commit gains one snapshot, so several gained ids should be impossible — but the code must
 * still pick one: the new current, else a ref target, else the newest.
 */
export const pickGainedSnapshotId = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
): string | null => {
  const previousIds = new Set(listSnapshotIds(previousFile));
  const gainedIds = listSnapshotIds(currentFile).filter(
    (snapshotId) => !previousIds.has(snapshotId),
  );

  const newCurrentId = gainedIds.find(
    (snapshotId) => snapshotId === currentFile.snapshot_id,
  );
  const refTargetId = gainedIds.find((snapshotId) =>
    isPointedAtByAnyRef(currentFile, snapshotId),
  );
  const newestId = gainedIds.at(-1);

  return newCurrentId ?? refTargetId ?? newestId ?? null;
};

export const countExpiredSnapshots = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
): number => {
  const currentIds = new Set(listSnapshotIds(currentFile));

  return listSnapshotIds(previousFile).filter(
    (snapshotId) => !currentIds.has(snapshotId),
  ).length;
};
