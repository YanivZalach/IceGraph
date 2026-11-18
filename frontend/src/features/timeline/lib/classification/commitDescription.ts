import type { SnapshotNode } from "../../api/nodeSchemas";
import type { ImpactSegment } from "../impactSegment";

export type CommitCategory =
  | "data-added"
  | "data-removed"
  | "data-replaced"
  | "unpublished"
  | "maintenance"
  | "definition"
  | "reference"
  | "rollback"
  | "quiet";

export interface Headline {
  title: string;
  category: CommitCategory;
}

export interface CommitDescription extends Headline {
  kind: "published-write" | "draft-write" | "re-point" | "metadata-only";
  impactSegments: ImpactSegment[];
  detailTexts: string[];
  snapshotId: string | null;
  branchName: string | null;
  repointTargetId: string | null;
  movedToBranchName: string | null;
}

export type SnapshotsById = ReadonlyMap<string, SnapshotNode>;
