import type { SnapshotNode } from "../../api/nodeSchemas";
import type { ImpactSegment } from "../impactSegment";

export interface CommitDescription {
  kind: "published-write" | "draft-write" | "re-point" | "metadata-only";
  title: string;
  impactSegments: ImpactSegment[];
  detailTexts: string[];
  snapshotId: string | null;
  branchName: string | null;
  repointTargetId: string | null;
  movedToBranchName: string | null;
}

export type SnapshotsById = ReadonlyMap<string, SnapshotNode>;
