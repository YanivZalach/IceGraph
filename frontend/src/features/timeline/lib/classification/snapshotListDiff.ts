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
  const isEitherListMissing =
    previousFile.pointed_snapshots_files === null ||
    currentFile.pointed_snapshots_files === null;
  if (isEitherListMissing) {
    return 0;
  }

  const currentIds = new Set(listSnapshotIds(currentFile));

  return listSnapshotIds(previousFile).filter(
    (snapshotId) => !currentIds.has(snapshotId),
  ).length;
};
