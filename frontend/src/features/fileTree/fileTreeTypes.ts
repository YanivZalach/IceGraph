import type { GraphNode } from "./fileTreeSchemas";

export type DataFileType = "data" | "position_delete" | "equality_delete";
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
  equalityDeleteFileCount: number;
  fileCount: number;
  largestSizeBytes: number;
  positionDeleteFileCount: number;
  smallestSizeBytes: number;
  totalRowCount: number;
  totalSizeBytes: number;
}

export interface FileTreeFolder {
  allFiles: DataFileNode[];
  children: FileTreeFolder[];
  directFiles: DataFileNode[];
  id: string;
  label: string;
  path: string;
  statistics: FileStatistics;
}

export interface PartitionGroup {
  files: DataFileNode[];
  id: string;
  name: string;
  statistics: FileStatistics;
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
  | { folder: FileTreeFolder; kind: "folder" }
  | { kind: "partition"; partition: PartitionGroup };
