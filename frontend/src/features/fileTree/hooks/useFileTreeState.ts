import { useState } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import type {
  DataFileNode,
  FileTreeViewMode,
  InspectedFileTreeItem,
  SnapshotFileScope,
} from "../types";

export interface FileTreeViewSettingsState {
  requestedBranchName: string | null;
  requestedSnapshotId: string | null;
  scope: SnapshotFileScope;
  search: string;
  setBranch: (branchName: string | null) => void;
  setScope: (scope: SnapshotFileScope) => void;
  setSearch: (search: string) => void;
  setSnapshot: (snapshotId: string) => void;
  setViewMode: (viewMode: FileTreeViewMode) => void;
  viewMode: FileTreeViewMode;
}

export interface FileTreeSelection {
  checkedFileIds: Set<string>;
  closeInspector: () => void;
  collapseAll: () => void;
  collapseItems: (itemIds: string[]) => void;
  expandItems: (itemIds: string[]) => void;
  expandedItemIds: Set<string>;
  inspect: (item: InspectedFileTreeItem) => void;
  inspectedItem: InspectedFileTreeItem | null;
  selectFiles: (files: DataFileNode[]) => void;
  toggleChecked: (fileId: string) => void;
  toggleExpanded: (itemId: string) => void;
  toggleFiles: (files: DataFileNode[]) => void;
}

interface FileTreeState {
  selection: FileTreeSelection;
  viewSettings: FileTreeViewSettingsState;
}

export const useFileTreeState = (): FileTreeState => {
  const navigate = useNavigate({ from: "/table/filetree" });
  const searchParameters = useSearch({ from: "/table/filetree" });
  const [expandedItemIds, setExpandedItemIds] = useState(new Set<string>());
  const [checkedFileIds, setCheckedFileIds] = useState(new Set<string>());
  const [inspectedItem, setInspectedItem] =
    useState<InspectedFileTreeItem | null>(null);

  const collapseAll = () => {
    setExpandedItemIds(new Set());
  };
  const clearTransientState = () => {
    collapseAll();
    setCheckedFileIds(new Set());
    setInspectedItem(null);
  };
  const selection: FileTreeSelection = {
    checkedFileIds,
    closeInspector: () => {
      setInspectedItem(null);
    },
    collapseAll,
    collapseItems: (itemIds) => {
      setExpandedItemIds((currentIds) => {
        const nextIds = new Set(currentIds);
        for (const itemId of itemIds) nextIds.delete(itemId);
        return nextIds;
      });
    },
    expandItems: (itemIds) => {
      setExpandedItemIds((currentIds) => new Set([...currentIds, ...itemIds]));
    },
    expandedItemIds,
    inspect: (item) => {
      setInspectedItem(item);
    },
    inspectedItem,
    selectFiles: (files) => {
      setCheckedFileIds(new Set(files.map(({ id }) => id)));
    },
    toggleChecked: (fileId) => {
      setCheckedFileIds((currentIds) => {
        const nextIds = new Set(currentIds);
        if (nextIds.has(fileId)) nextIds.delete(fileId);
        else nextIds.add(fileId);
        return nextIds;
      });
    },
    toggleExpanded: (itemId) => {
      setExpandedItemIds((currentIds) => {
        const nextIds = new Set(currentIds);
        if (nextIds.has(itemId)) nextIds.delete(itemId);
        else nextIds.add(itemId);
        return nextIds;
      });
    },
    toggleFiles: (files) => {
      const shouldClear = files.every((file) => checkedFileIds.has(file.id));
      setCheckedFileIds((currentIds) => {
        const nextIds = new Set(currentIds);
        for (const file of files) {
          if (shouldClear) nextIds.delete(file.id);
          else nextIds.add(file.id);
        }
        return nextIds;
      });
    },
  };

  const viewSettings: FileTreeViewSettingsState = {
    requestedBranchName:
      searchParameters.filetree_branch === undefined
        ? "main"
        : searchParameters.filetree_branch === ""
          ? null
          : searchParameters.filetree_branch,
    requestedSnapshotId: searchParameters.filetree_snapshot_id ?? null,
    scope: searchParameters.filetree_scope ?? "snapshot",
    search: searchParameters.filetree_search ?? "",
    setBranch: (branchName) => {
      void navigate({
        search: (previous) => ({
          ...previous,
          filetree_branch: branchName ?? "",
          filetree_snapshot_id: undefined,
        }),
      });
      clearTransientState();
    },
    setScope: (nextScope) => {
      void navigate({
        search: (previous) => ({
          ...previous,
          filetree_scope: nextScope === "snapshot" ? undefined : nextScope,
        }),
      });
      clearTransientState();
    },
    setSearch: (nextSearch) => {
      void navigate({
        replace: true,
        search: (previous) => ({
          ...previous,
          filetree_search: nextSearch === "" ? undefined : nextSearch,
        }),
      });
    },
    setSnapshot: (snapshotId) => {
      void navigate({
        search: (previous) => ({
          ...previous,
          filetree_snapshot_id: snapshotId,
        }),
      });
      clearTransientState();
    },
    setViewMode: (nextViewMode) => {
      void navigate({
        search: (previous) => ({
          ...previous,
          filetree_grouping: nextViewMode === "tree" ? undefined : nextViewMode,
        }),
      });
      collapseAll();
    },
    viewMode: searchParameters.filetree_grouping ?? "tree",
  };

  return { selection, viewSettings };
};
