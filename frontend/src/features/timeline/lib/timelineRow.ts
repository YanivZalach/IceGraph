import type { MetadataFileNode, SnapshotNode } from "../api/nodeSchemas";
import type { BranchGraph } from "./buildBranchLanes";
import type { CommitCategory } from "./classification/commitDescription";
import { formatUnreadableEvents } from "./format/formatUnreadableEvents";
import type { ImpactSegment } from "./impactSegment";

export interface RefBadge {
  name: string;
  type: "branch" | "tag";
}

export interface UnreadableFileSummary {
  path: string;
  timestampMs: number;
  reasons: string[];
}

export interface CommitRow {
  kind:
    | "published-write"
    | "draft-write"
    | "re-point"
    | "metadata-only"
    | "unknown-change";
  title: string;
  category: CommitCategory;
  impact: ImpactSegment[];
  details: string[];
  repointTargetId: string | null;
  shortId: string;
  snapshotId: string | null;
  filePath: string;
  previousFilePath: string | null;
  timestampMs: number;
  badges: RefBadge[];
  branchName: string | null;
  movedToBranchName: string | null;
  isDraftPublishedLater: boolean;
  publishedAsSnapshotId: string | null;
  publishedAtMs: number | null;
}

/** `filePath` and `timestampMs` are its newest file's. */
export interface GapRow {
  kind: "gap";
  filePath: string;
  timestampMs: number;
  unreadableFiles: UnreadableFileSummary[];
}

export type TimelineRow = CommitRow | GapRow;

export const titleOf = (row: TimelineRow): string =>
  row.kind === "gap"
    ? formatUnreadableEvents(row.unreadableFiles.length)
    : row.title;

export interface TimelineData {
  rows: TimelineRow[];
  unreadableCommitCount: number;
  isTableMetadataUnreadable: boolean;
  olderCommitCount: number;
  snapshotsById: ReadonlyMap<string, SnapshotNode>;
  filesByPath: ReadonlyMap<string, MetadataFileNode>;
  branchGraph: BranchGraph;
}

export const commitRowDefaults = (
  file: MetadataFileNode,
): Omit<CommitRow, "kind" | "title" | "category"> => ({
  impact: [],
  details: [],
  repointTargetId: null,
  shortId: "",
  snapshotId: null,
  filePath: file.file_path,
  previousFilePath: null,
  timestampMs: file.timestamp,
  badges: [],
  branchName: null,
  movedToBranchName: null,
  isDraftPublishedLater: false,
  publishedAsSnapshotId: null,
  publishedAtMs: null,
});
