import {
  metadataFileNodeSchema,
  nodeTypeSchema,
  snapshotNodeSchema,
  type MetadataFileNode,
  type SnapshotNode,
} from "../api/nodeSchemas";

export interface ParsedTimelineNodes {
  metadataFiles: MetadataFileNode[];
  snapshotsById: Map<string, SnapshotNode>;
  unreadableCommitCount: number;
}

export const parseTimelineNodes = (nodes: unknown[]): ParsedTimelineNodes => {
  const metadataFiles: MetadataFileNode[] = [];
  const snapshotsById = new Map<string, SnapshotNode>();
  let unreadableCommitCount = 0;

  for (const node of nodes) {
    const typedNode = nodeTypeSchema.safeParse(node);
    if (!typedNode.success) {
      unreadableCommitCount += 1;
      continue;
    }

    if (
      typedNode.data.type === "metadata" ||
      typedNode.data.type === "main_metadata"
    ) {
      const metadataFile = metadataFileNodeSchema.safeParse(node);
      // An unreadable metadata file is skipped so its neighbors merge into one
      // degraded row. Errored snapshots
      // stay: their row renders and the details panel shows the error banner.
      if (metadataFile.success && metadataFile.data.error == null) {
        metadataFiles.push(metadataFile.data);
      } else {
        unreadableCommitCount += 1;
      }
      continue;
    }

    if (typedNode.data.type === "snapshot") {
      const snapshot = snapshotNodeSchema.safeParse(node);
      if (snapshot.success) {
        snapshotsById.set(snapshot.data.snapshot_id, snapshot.data);
      }
    }
  }

  return { metadataFiles, snapshotsById, unreadableCommitCount };
};
