import { z } from "zod";
import { utcTimestampSchema } from "../api/utcTimestampSchema";
import {
  metadataFileNodeSchema,
  nodeTypeSchema,
  snapshotNodeSchema,
  type MetadataFileNode,
  type SnapshotNode,
} from "../api/nodeSchemas";
import type { ChainedFile } from "./orderMetadataFiles";

const unreadableFilePlacementSchema = z.object({
  file_path: z.string(),
  previous_file: z.string().nullish(),
  timestamp: utcTimestampSchema,
  errors: z.array(z.string()).catch([]),
});

const MAX_SCHEMA_ISSUES = 3;

export interface UnreadableFile extends ChainedFile {
  reasons: string[];
}

export interface ParsedTimelineNodes {
  metadataFiles: MetadataFileNode[];
  snapshotsById: Map<string, SnapshotNode>;
  unreadableCommitCount: number;
  unreadableFiles: UnreadableFile[];
}

export const parseTimelineNodes = (nodes: unknown[]): ParsedTimelineNodes => {
  const metadataFiles: MetadataFileNode[] = [];
  const snapshotsById = new Map<string, SnapshotNode>();
  const unreadableFiles: UnreadableFile[] = [];
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
      // Errored snapshots stay: their row renders and the details panel shows
      // the error banner. An unreadable metadata file keeps only its place in
      // the chain, so the next readable commit can say what it cannot see.
      if (metadataFile.success && metadataFile.data.errors.length === 0) {
        metadataFiles.push(metadataFile.data);
      } else {
        unreadableCommitCount += 1;
        const placement = unreadableFilePlacementSchema.safeParse(node);
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
            reasons: [...placement.data.errors, ...schemaIssues],
          });
        }
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

  return {
    metadataFiles,
    snapshotsById,
    unreadableCommitCount,
    unreadableFiles,
  };
};
