import type { GraphNode } from "./schemas";

export type DataFileType =
  "data" | "position_delete" | "deletion_vector" | "equality_delete";
export type FileTreeViewMode = "flat" | "tree";
export type SnapshotFileScope = "commit" | "snapshot";

export interface DataFileNode extends GraphNode {
  type: DataFileType;
}

export interface SnapshotNode extends GraphNode {
  type: "snapshot";
}

export interface FileStatistics {
  averageSizeBytes: number;
  dataFileCount: number;
  dataRowCount: bigint | null;
  deletionVectorCount: number;
  deletionVectorPositionCount: bigint | null;
  equalityDeleteFileCount: number;
  equalityDeleteRecordCount: bigint | null;
  fileCount: number;
  hasCompleteFileSizes: boolean;
  largestSizeBytes: number;
  positionDeleteFileCount: number;
  positionDeleteRecordCount: bigint | null;
  smallestSizeBytes: number;
  totalSizeBytes: number;
}

export interface PartitionPathNode {
  allFiles: DataFileNode[];
  children: PartitionPathNode[];
  directFiles: DataFileNode[];
  id: string;
  label: string;
  path: string;
}

export interface PartitionPathSegment {
  key: string;
  label: string;
}

export interface PartitionGroup {
  files: DataFileNode[];
  id: string;
  name: string;
  segments: PartitionPathSegment[];
}

export interface Branch {
  headSnapshotId: string;
  name: string;
}

export interface GraphConnection {
  isDeleted: boolean;
  to: string;
}

export interface FileTreeGraphIndex {
  adjacencyByNodeId: Record<string, GraphConnection[]>;
  nodesById: Record<string, GraphNode>;
  snapshots: SnapshotNode[];
  snapshotsBySnapshotId: Record<string, SnapshotNode>;
}

export type InspectedFileTreeItem =
  | { file: DataFileNode; kind: "file" }
  | { kind: "partition-path"; partitionPathNode: PartitionPathNode }
  | { kind: "partition"; partition: PartitionGroup };
