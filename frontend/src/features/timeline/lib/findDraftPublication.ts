import type { MetadataFileNode } from "../api/nodeSchemas";
import type { SnapshotsById } from "./classification/commitDescription";

export interface DraftPublication {
  isDraftPublishedLater: boolean;
  publishedAsSnapshotId: string | null;
  publishedAtMs: number | null;
}

const NOT_PUBLISHED: DraftPublication = {
  isDraftPublishedLater: false,
  publishedAsSnapshotId: null,
  publishedAtMs: null,
};

const isPointingAt = (file: MetadataFileNode, snapshotId: string): boolean =>
  file.snapshot_id === snapshotId ||
  Object.values(file.refs).some((ref) => ref["snapshot-id"] === snapshotId);

/**
 * A cherry-pick publishes a copy, so the draft itself never fills. `wap.id`/`published-wap-id`
 * exist only for WAP flows; `source-snapshot-id` is stamped on every cherry-pick.
 */
export const findDraftPublication = (
  draftSnapshotId: string,
  laterFiles: MetadataFileNode[],
  snapshotsById: SnapshotsById,
): DraftPublication => {
  const directPublishFile = laterFiles.find((file) =>
    isPointingAt(file, draftSnapshotId),
  );
  if (directPublishFile !== undefined) {
    return {
      isDraftPublishedLater: true,
      publishedAsSnapshotId: null,
      publishedAtMs: directPublishFile.timestamp,
    };
  }

  const draftWapId = snapshotsById.get(draftSnapshotId)?.summary["wap.id"];

  for (const file of laterFiles) {
    if (file.snapshot_id === null) {
      continue;
    }
    const laterSnapshot = snapshotsById.get(file.snapshot_id);
    if (laterSnapshot === undefined) {
      continue;
    }

    const isWapMatch =
      draftWapId !== undefined &&
      laterSnapshot.summary["published-wap-id"] === draftWapId;
    const isSourceMatch =
      laterSnapshot.summary["source-snapshot-id"] === draftSnapshotId;

    if (isWapMatch || isSourceMatch) {
      return {
        isDraftPublishedLater: false,
        publishedAsSnapshotId: laterSnapshot.snapshot_id,
        publishedAtMs: file.timestamp,
      };
    }
  }

  return NOT_PUBLISHED;
};
