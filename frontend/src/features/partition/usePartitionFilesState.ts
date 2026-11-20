import { useState } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import type { DataFileNode, InspectedFileTreeItem } from "../fileTree/types";

const GROUP_BY_SEPARATOR = ",";

const decodeFieldName = (fieldName: string): string => {
  try {
    return decodeURIComponent(fieldName);
  } catch {
    return fieldName;
  }
};

export const usePartitionFilesState = () => {
  const navigate = useNavigate({ from: "/partition/filetree" });
  const search = useSearch({ from: "/partition/filetree" });
  const requestedGroupByFields =
    search.filetree_group_by === undefined || search.filetree_group_by === ""
      ? []
      : search.filetree_group_by.split(GROUP_BY_SEPARATOR).map(decodeFieldName);
  const fileSearch = search.filetree_search ?? "";
  const [expandedItemIds, setExpandedItemIds] = useState<Set<string> | null>(
    null,
  );
  const [checkedFileIds, setCheckedFileIds] = useState(new Set<string>());
  const [inspectedItem, setInspectedItem] =
    useState<InspectedFileTreeItem | null>(null);

  const toggleIds = (ids: Set<string>, itemIds: string[]): Set<string> => {
    const nextIds = new Set(ids);
    for (const itemId of itemIds) {
      if (nextIds.has(itemId)) nextIds.delete(itemId);
      else nextIds.add(itemId);
    }
    return nextIds;
  };

  return {
    checkedFileIds,
    expandedItemIds,
    fileSearch,
    inspectedItem,
    requestedGroupByFields,
    setExpandedItemIds,
    setInspectedItem,
    setGroupByFields: (groupByFields: string[]) => {
      void navigate({
        search: (previous) => ({
          ...previous,
          filetree_group_by:
            groupByFields.length === 0
              ? undefined
              : groupByFields
                  .map((fieldName) => encodeURIComponent(fieldName))
                  .join(GROUP_BY_SEPARATOR),
        }),
      });
      setExpandedItemIds(null);
    },
    setFileSearch: (nextSearch: string) => {
      void navigate({
        replace: true,
        search: (previous) => ({
          ...previous,
          filetree_search: nextSearch === "" ? undefined : nextSearch,
        }),
      });
    },
    toggleChecked: (fileId: string) => {
      setCheckedFileIds((currentIds) => toggleIds(currentIds, [fileId]));
    },
    toggleFiles: (files: DataFileNode[]) => {
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
    selectFiles: (files: DataFileNode[]) => {
      setCheckedFileIds(new Set(files.map(({ id }) => id)));
    },
  };
};
