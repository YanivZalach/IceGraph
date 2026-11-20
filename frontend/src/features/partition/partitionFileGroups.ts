import { buildPartitionPathTree } from "../fileTree/model/partitionModel";
import type {
  DataFileNode,
  PartitionGroup,
  PartitionPathNode,
  PartitionPathSegment,
} from "../fileTree/types";
import {
  formatPartitionFieldLabel,
  formatPartitionLabel,
} from "../table/partitionLabel";

const MATCHED_FILES_LABEL = "Matched files";
const UNDECIDED_FILES_LABEL = "Undecided files";

const groupSegment = (
  file: DataFileNode,
  fieldName: string,
): PartitionPathSegment => {
  for (const field of file.details.partition ?? []) {
    if (field.name === fieldName) {
      return {
        key: JSON.stringify([field.name, field.value]),
        label: formatPartitionFieldLabel(field),
      };
    }
  }
  return {
    key: JSON.stringify([fieldName, "not in spec"]),
    label: `${fieldName}: not in spec`,
  };
};

const matchesSearch = (file: DataFileNode, search: string): boolean =>
  search === "" ||
  [file.id, formatPartitionLabel(file.details.partition ?? [])].some((text) =>
    text.toLowerCase().includes(search),
  );

export interface PartitionFileGroups {
  partitions: PartitionGroup[];
  partitionPathNodes: PartitionPathNode[];
}

export const groupPartitionFiles = (
  files: DataFileNode[],
  groupByFields: string[],
  search: string,
): PartitionFileGroups => {
  const normalizedSearch = search.trim().toLowerCase();
  const groupsByKey = new Map<string, PartitionGroup>();
  for (const file of files) {
    if (!matchesSearch(file, normalizedSearch)) continue;
    const rootLabel =
      file.details.partition_match === "undecided"
        ? UNDECIDED_FILES_LABEL
        : MATCHED_FILES_LABEL;
    const segments = [
      { key: rootLabel, label: rootLabel },
      ...groupByFields.map((fieldName) => groupSegment(file, fieldName)),
    ];
    const key = JSON.stringify(segments.map((segment) => segment.key));
    const existingGroup = groupsByKey.get(key);
    if (existingGroup === undefined) {
      groupsByKey.set(key, {
        files: [file],
        id: `partition:${key}`,
        name: segments.map((segment) => segment.label).join(" / "),
        segments,
      });
    } else {
      existingGroup.files.push(file);
    }
  }
  const partitions = [...groupsByKey.values()];
  return {
    partitions,
    partitionPathNodes: buildPartitionPathTree(partitions).sort(
      (first, second) =>
        Number(second.label === MATCHED_FILES_LABEL) -
        Number(first.label === MATCHED_FILES_LABEL),
    ),
  };
};

export const listGroupableFields = (
  files: DataFileNode[],
  filteredFieldNames: string[],
): string[] => {
  const fieldNames = new Set<string>();
  for (const file of files) {
    for (const field of file.details.partition ?? []) {
      if (!filteredFieldNames.includes(field.name)) fieldNames.add(field.name);
    }
  }
  return [...fieldNames];
};
