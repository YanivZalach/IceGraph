import type { FileTreeContext } from "../schemas";
import type { Branch, FileTreeGraphIndex, SnapshotNode } from "../types";
import { getTimestampSortValue } from "./graphIndex";

interface BranchReference {
  "snapshot-id": string;
  type: string;
}

interface VersionedBranch extends Branch {
  timestamp: number;
}

interface CurrentSnapshotSelection {
  snapshot: SnapshotNode | undefined;
  warnings: string[];
}

export const getBranches = (context: FileTreeContext): Branch[] => {
  const branchesByName = new Map<string, VersionedBranch>();
  const recordBranch = (
    name: string,
    reference: BranchReference,
    timestamp: number,
  ) => {
    if (reference.type !== "branch") return;
    const existing = branchesByName.get(name);
    if (existing !== undefined && existing.timestamp > timestamp) return;
    branchesByName.set(name, {
      headSnapshotId: reference["snapshot-id"],
      name,
      timestamp,
    });
  };

  for (const node of context.nodes) {
    if (node.type !== "metadata" && node.type !== "main_metadata") continue;
    for (const [name, reference] of Object.entries(node.details.refs ?? {})) {
      recordBranch(name, reference, getTimestampSortValue(node));
    }
  }
  for (const [name, reference] of Object.entries(
    context.metadata?.refs ?? {},
  )) {
    recordBranch(name, reference, Number.POSITIVE_INFINITY);
  }

  return [...branchesByName.values()]
    .map(({ headSnapshotId, name }) => ({ headSnapshotId, name }))
    .sort((first, second) => first.name.localeCompare(second.name));
};

export const getDisplayedSnapshots = (
  graphIndex: FileTreeGraphIndex,
  branches: Branch[],
  selectedBranchName: string | null,
): SnapshotNode[] => {
  if (selectedBranchName === null) return graphIndex.snapshots;
  const branch = branches.find(({ name }) => name === selectedBranchName);
  if (branch === undefined) return graphIndex.snapshots;

  const branchSnapshots: SnapshotNode[] = [];
  const visitedSnapshotIds = new Set<string>();
  let currentSnapshotId: string | null | undefined = branch.headSnapshotId;
  while (
    currentSnapshotId != null &&
    !visitedSnapshotIds.has(currentSnapshotId)
  ) {
    visitedSnapshotIds.add(currentSnapshotId);
    const snapshot: SnapshotNode | undefined =
      graphIndex.snapshotsBySnapshotId[currentSnapshotId];
    if (snapshot === undefined) break;
    branchSnapshots.push(snapshot);
    currentSnapshotId = snapshot.details.parent_id;
  }
  return branchSnapshots.reverse();
};

export const selectCurrentSnapshot = (
  snapshots: SnapshotNode[],
  requestedSnapshotId: string | null,
): CurrentSnapshotSelection => {
  if (requestedSnapshotId !== null) {
    const requestedSnapshot = snapshots.find(
      (snapshot) =>
        (snapshot.details.snapshot_id ?? snapshot.id) === requestedSnapshotId,
    );
    if (requestedSnapshot !== undefined) {
      return { snapshot: requestedSnapshot, warnings: [] };
    }
  }

  const latestSnapshot = snapshots.at(-1);
  if (requestedSnapshotId === null || latestSnapshot === undefined) {
    return { snapshot: latestSnapshot, warnings: [] };
  }
  const latestSnapshotId =
    latestSnapshot.details.snapshot_id ?? latestSnapshot.id;
  return {
    snapshot: latestSnapshot,
    warnings: [
      `Requested snapshot ${requestedSnapshotId} is unavailable in this branch or loaded range. Showing latest snapshot ${latestSnapshotId}.`,
    ],
  };
};

export const formatSnapshotVersion = (
  snapshot: SnapshotNode,
  isLatest: boolean,
): string => {
  const snapshotId = snapshot.details.snapshot_id ?? snapshot.id;
  const rawOperation = snapshot.details.operation_description;
  const operation =
    typeof rawOperation === "string" && rawOperation.trim() !== ""
      ? rawOperation
      : "Snapshot";
  const timestamp = snapshot.details.timestamp;
  const timestampLabel =
    typeof timestamp === "string" || typeof timestamp === "number"
      ? String(timestamp)
      : "Unknown time";
  return `ID ${snapshotId} · ${operation} · ${timestampLabel}${isLatest ? " · latest" : ""}`;
};
