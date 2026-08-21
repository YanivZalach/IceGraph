import type { MetadataFileNode } from "../api/nodeSchemas";

export interface RefBadge {
  name: string;
  type: "branch" | "tag";
}

export interface TimelineRow {
  kind:
    | "published-write"
    | "draft-write"
    | "re-point"
    | "metadata-only"
    | "boundary";
  title: string;
  impact: string;
  shortId: string;
  snapshotId: string | null;
  filePath: string;
  timestampMs: number;
  badges: RefBadge[];
  branchName: string | null;
  isDraftPublishedLater: boolean;
  publishedAsSnapshotId: string | null;
  publishedAtMs: number | null;
}

export interface TimelineData {
  rows: TimelineRow[];
  skippedNodeCount: number;
}

export const boundaryRow = (file: MetadataFileNode): TimelineRow => ({
  kind: "boundary",
  title: "",
  impact: "",
  shortId: "",
  snapshotId: null,
  filePath: file.file_path,
  timestampMs: file.timestamp,
  badges: [],
  branchName: null,
  isDraftPublishedLater: false,
  publishedAsSnapshotId: null,
  publishedAtMs: null,
});
