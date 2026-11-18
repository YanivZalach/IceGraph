import {
  metadataFileNodeSchema,
  nodeTypeSchema,
  snapshotNodeSchema,
  unreadableMetadataFileSchema,
  type MetadataFileNode,
  type SnapshotNode,
} from "../api/nodeSchemas";
import type { ChainedFile } from "./orderMetadataFiles";

const MAX_SCHEMA_ISSUES = 3;

export interface UnreadableFile extends ChainedFile {
  reasons: string[];
}

export interface ParsedTimelineNodes {
  readableFiles: MetadataFileNode[];
  unreadableFiles: UnreadableFile[];
  snapshotsById: Map<string, SnapshotNode>;
  unreadableCommitCount: number;
}

export const parseTimelineNodes = (nodes: unknown[]): ParsedTimelineNodes => {
  const readableFiles: MetadataFileNode[] = [];
  const unreadableFiles: UnreadableFile[] = [];
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
      if (metadataFile.success && metadataFile.data.errors.length === 0) {
        readableFiles.push(metadataFile.data);
        continue;
      }

      unreadableCommitCount += 1;
      const placement = unreadableMetadataFileSchema.safeParse(node);
      if (placement.success) {
        const schemaIssues = metadataFile.success
          ? []
          : metadataFile.error.issues
              .slice(0, MAX_SCHEMA_ISSUES)
              .map(
                (issue) =>
                  `${issue.path.join(".") || "file"}: ${issue.message}`,
              );
        unreadableFiles.push({
          file_path: placement.data.file_path,
          previous_file: placement.data.previous_file ?? null,
          timestamp: placement.data.timestamp,
          reasons: [...new Set([...placement.data.errors, ...schemaIssues])],
        });
      }
      continue;
    }

    // Errored snapshots stay: their row renders and the details panel shows
    // the error banner.
    if (typedNode.data.type === "snapshot") {
      const snapshot = snapshotNodeSchema.safeParse(node);
      if (snapshot.success) {
        snapshotsById.set(snapshot.data.snapshot_id, snapshot.data);
      }
    }
  }

  return {
    readableFiles,
    unreadableFiles,
    snapshotsById,
    unreadableCommitCount,
  };
};
