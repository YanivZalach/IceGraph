import { useState } from "react";
import type {
  DataFileNode,
  FileTreeFolder,
  FileTreeViewMode,
  InspectedFileTreeItem,
  PartitionGroup,
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
  inspectFolder: (folder: FileTreeFolder) => void;
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
  const [search, setSearchState] = useState("");
  const [requestedBranchName, setRequestedBranchName] = useState<string | null>(
    "main",
  );
  const [requestedSnapshotId, setRequestedSnapshotId] = useState<string | null>(
    null,
  );
  const [scope, setScopeState] = useState<SnapshotFileScope>("snapshot");
  const [viewMode, setViewModeState] = useState<FileTreeViewMode>("tree");
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
    setRequestedBranchName(branchName);
    setRequestedSnapshotId(null);
    clearTransientState();
  };
  const setSnapshot = (snapshotId: string) => {
    setRequestedSnapshotId(snapshotId);
    clearTransientState();
  };
  const setScope = (nextScope: SnapshotFileScope) => {
    setScopeState(nextScope);
    clearTransientState();
  };
  const setViewMode = (nextViewMode: FileTreeViewMode) => {
    setViewModeState(nextViewMode);
    setExpandedItemIds(new Set());
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
    inspectFolder: (folder) => {
      setInspectedItem({ folder, kind: "folder" });
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
    setSearch: setSearchState,
    setSnapshot,
    setViewMode,
    toggleChecked,
    toggleExpanded,
    toggleFiles,
    viewMode,
  };
};
