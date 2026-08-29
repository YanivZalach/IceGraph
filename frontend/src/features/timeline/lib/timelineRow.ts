import type { MetadataFileNode, SnapshotNode } from "../api/nodeSchemas";
import type { ImpactSegment } from "./impactSegment";

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
  impact: ImpactSegment[];
  details: string[];
  repointTargetId: string | null;
  shortId: string;
  snapshotId: string | null;
  filePath: string;
  timestampMs: number;
  badges: RefBadge[];
  branchName: string | null;
  movedToBranchName: string | null;
  isDraftPublishedLater: boolean;
  publishedAsSnapshotId: string | null;
  publishedAtMs: number | null;
}

export interface TimelineData {
  rows: TimelineRow[];
  skippedNodeCount: number;
  olderCommitCount: number;
  snapshotsById: ReadonlyMap<string, SnapshotNode>;
  filesByPath: ReadonlyMap<string, MetadataFileNode>;
}

export const boundaryRow = (file: MetadataFileNode): TimelineRow => ({
  kind: "boundary",
  title: "",
  impact: [],
  details: [],
  repointTargetId: null,
  shortId: "",
  snapshotId: null,
  filePath: file.file_path,
  timestampMs: file.timestamp,
  badges: [],
  branchName: null,
  movedToBranchName: null,
  isDraftPublishedLater: false,
  publishedAsSnapshotId: null,
  publishedAtMs: null,
});
