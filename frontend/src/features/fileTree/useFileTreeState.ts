import { useState } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import type {
  DataFileNode,
  FileTreeViewMode,
  InspectedFileTreeItem,
  PartitionGroup,
  PartitionPathNode,
  SnapshotFileScope,
} from "./types";

interface FileTreeState {
  checkedFileIds: Set<string>;
  closeInspector: () => void;
  collapseAll: () => void;
  collapseMany: (itemIds: string[]) => void;
  expandItems: (itemIds: string[]) => void;
  expandedItemIds: Set<string>;
  inspectFile: (file: DataFileNode) => void;
  inspectedItem: InspectedFileTreeItem | null;
  inspectPartitionPathNode: (node: PartitionPathNode) => void;
  inspectPartition: (partition: PartitionGroup) => void;
  requestedBranchName: string | null;
  requestedSnapshotId: string | null;
  scope: SnapshotFileScope;
  search: string;
  selectFiles: (files: DataFileNode[]) => void;
  setBranch: (branchName: string | null) => void;
  setScope: (scope: SnapshotFileScope) => void;
  setSearch: (search: string) => void;
  setSnapshot: (snapshotId: string) => void;
  setViewMode: (viewMode: FileTreeViewMode) => void;
  toggleChecked: (fileId: string) => void;
  toggleExpanded: (itemId: string) => void;
  toggleFiles: (files: DataFileNode[]) => void;
  viewMode: FileTreeViewMode;
}

export const useFileTreeState = (): FileTreeState => {
  const navigate = useNavigate({ from: "/table/filetree" });
  const searchParameters = useSearch({ from: "/table/filetree" });
  const requestedBranchName =
    searchParameters.filetree_branch === undefined
      ? "main"
      : searchParameters.filetree_branch === ""
        ? null
        : searchParameters.filetree_branch;
  const requestedSnapshotId = searchParameters.filetree_snapshot_id ?? null;
  const scope = searchParameters.filetree_scope ?? "snapshot";
  const search = searchParameters.filetree_search ?? "";
  const viewMode = searchParameters.filetree_grouping ?? "tree";
  const [expandedItemIds, setExpandedItemIds] = useState(new Set<string>());
  const [checkedFileIds, setCheckedFileIds] = useState(new Set<string>());
  const [inspectedItem, setInspectedItem] =
    useState<InspectedFileTreeItem | null>(null);

  const clearTransientState = () => {
    setExpandedItemIds(new Set());
    setCheckedFileIds(new Set());
    setInspectedItem(null);
  };
  const setBranch = (branchName: string | null) => {
    void navigate({
      search: (previous) => ({
        ...previous,
        filetree_branch: branchName ?? "",
        filetree_snapshot_id: undefined,
      }),
    });
    clearTransientState();
  };
  const setSnapshot = (snapshotId: string) => {
    void navigate({
      search: (previous) => ({
        ...previous,
        filetree_snapshot_id: snapshotId,
      }),
    });
    clearTransientState();
  };
  const setScope = (nextScope: SnapshotFileScope) => {
    void navigate({
      search: (previous) => ({
        ...previous,
        filetree_scope: nextScope === "snapshot" ? undefined : nextScope,
      }),
    });
    clearTransientState();
  };
  const setViewMode = (nextViewMode: FileTreeViewMode) => {
    void navigate({
      search: (previous) => ({
        ...previous,
        filetree_grouping: nextViewMode === "tree" ? undefined : nextViewMode,
      }),
    });
    setExpandedItemIds(new Set());
  };
  const setSearch = (nextSearch: string) => {
    void navigate({
      replace: true,
      search: (previous) => ({
        ...previous,
        filetree_search: nextSearch === "" ? undefined : nextSearch,
      }),
    });
  };
  const toggleExpanded = (itemId: string) => {
    setExpandedItemIds((currentIds) => {
      const nextIds = new Set(currentIds);
      if (nextIds.has(itemId)) nextIds.delete(itemId);
      else nextIds.add(itemId);
      return nextIds;
    });
  };
  const expandItems = (itemIds: string[]) => {
    setExpandedItemIds((currentIds) => new Set([...currentIds, ...itemIds]));
  };
  const collapseMany = (itemIds: string[]) => {
    setExpandedItemIds((currentIds) => {
      const nextIds = new Set(currentIds);
      for (const itemId of itemIds) nextIds.delete(itemId);
      return nextIds;
    });
  };
  const toggleChecked = (fileId: string) => {
    setCheckedFileIds((currentIds) => {
      const nextIds = new Set(currentIds);
      if (nextIds.has(fileId)) nextIds.delete(fileId);
      else nextIds.add(fileId);
      return nextIds;
    });
  };
  const toggleFiles = (files: DataFileNode[]) => {
    const shouldClear = files.every((file) => checkedFileIds.has(file.id));
    setCheckedFileIds((currentIds) => {
      const nextIds = new Set(currentIds);
      for (const file of files) {
        if (shouldClear) nextIds.delete(file.id);
        else nextIds.add(file.id);
      }
      return nextIds;
    });
  };

  return {
    checkedFileIds,
    closeInspector: () => {
      setInspectedItem(null);
    },
    collapseAll: () => {
      setExpandedItemIds(new Set());
    },
    collapseMany,
    expandItems,
    expandedItemIds,
    inspectFile: (file) => {
      setInspectedItem({ file, kind: "file" });
    },
    inspectedItem,
    inspectPartitionPathNode: (partitionPathNode) => {
      setInspectedItem({ kind: "partition-path", partitionPathNode });
    },
    inspectPartition: (partition) => {
      setInspectedItem({ kind: "partition", partition });
    },
    requestedBranchName,
    requestedSnapshotId,
    scope,
    search,
    selectFiles: (files) => {
      setCheckedFileIds(new Set(files.map(({ id }) => id)));
    },
    setBranch,
    setScope,
    setSearch,
    setSnapshot,
    setViewMode,
    toggleChecked,
    toggleExpanded,
    toggleFiles,
    viewMode,
  };
};
