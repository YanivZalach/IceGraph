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
  skippedNodeCount: number;
}

const hasBackendReadError = (node: {
  error?: string | null | undefined;
}): boolean => node.error !== null && node.error !== undefined;

export const parseTimelineNodes = (nodes: unknown[]): ParsedTimelineNodes => {
  const metadataFiles: MetadataFileNode[] = [];
  const snapshotsById = new Map<string, SnapshotNode>();
  let skippedNodeCount = 0;

  for (const node of nodes) {
    const typedNode = nodeTypeSchema.safeParse(node);
    if (!typedNode.success) {
      skippedNodeCount += 1;
      continue;
    }

    if (
      typedNode.data.type === "metadata" ||
      typedNode.data.type === "main_metadata"
    ) {
      const metadataFile = metadataFileNodeSchema.safeParse(node);
      if (!metadataFile.success || hasBackendReadError(metadataFile.data)) {
        skippedNodeCount += 1;
      } else {
        metadataFiles.push(metadataFile.data);
      }
      continue;
    }

    if (typedNode.data.type === "snapshot") {
      const snapshot = snapshotNodeSchema.safeParse(node);
      if (!snapshot.success || hasBackendReadError(snapshot.data)) {
        skippedNodeCount += 1;
      } else {
        snapshotsById.set(snapshot.data.snapshot_id, snapshot.data);
      }
    }
  }

  return { metadataFiles, snapshotsById, skippedNodeCount };
};
