import type { SnapshotNode } from "../../api/nodeSchemas";

export interface CommitDescription {
  kind: "published-write" | "draft-write" | "re-point" | "metadata-only";
  title: string;
  impactSegments: string[];
  snapshotId: string | null;
  branchName: string | null;
  repointTargetId: string | null;
}

export type SnapshotsById = ReadonlyMap<string, SnapshotNode>;
