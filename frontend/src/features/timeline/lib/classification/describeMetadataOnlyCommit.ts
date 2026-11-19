import type { MetadataFileNode, SnapshotRefs } from "../../api/nodeSchemas";
import type { TableMetadata } from "../../api/tableMetadataSchema";
import type { CommitDescription, Headline } from "./commitDescription";
import { impactText } from "../impactSegment";
import {
  sameTextChange,
  type DescribedChange,
} from "../definitionChanges/describedChange";
import { describePartitionSpecChange } from "../definitionChanges/describePartitionSpecChange";
import {
  describeIgnoredPropertyChanges,
  describePropertyChanges,
} from "../definitionChanges/describePropertyChanges";
import { describeRefChanges } from "../definitionChanges/describeRefChanges";
import { describeSchemaChange } from "../definitionChanges/describeSchemaChange";
import { describeSortOrderChange } from "../definitionChanges/describeSortOrderChange";
import {
  describeStatisticsChanges,
  type StatisticsChange,
} from "../definitionChanges/describeStatisticsChanges";
import { FIELD_TITLES } from "../details/describeFieldDiff";
import { diffMetadataFiles } from "../details/diffMetadataFiles";

export const UNRECOGNIZED_METADATA_TITLE = "Metadata updated";

const describeExpiredSnapshots = (count: number): DescribedChange[] => {
  if (count === 0) {
    return [];
  }

  const versions = count === 1 ? "version" : "versions";
  return [sameTextChange(`${count.toString()} old ${versions} removed`)];
};

const isTagChange = (
  previousRefs: SnapshotRefs,
  currentRefs: SnapshotRefs,
): boolean => {
  const changedEntry =
    Object.entries(currentRefs).find(
      ([name, ref]) =>
        JSON.stringify(previousRefs[name]) !== JSON.stringify(ref),
    ) ??
    Object.entries(previousRefs).find(
      ([name]) => currentRefs[name] === undefined,
    );

  return changedEntry?.[1].type === "tag";
};

const statisticsHeadline = (changes: StatisticsChange[]): Headline => {
  if (changes.every((change) => change.action === "added")) {
    return { title: "Statistics added", category: "maintenance" };
  }
  if (changes.every((change) => change.action === "removed")) {
    return { title: "Statistics removed", category: "maintenance" };
  }
  return { title: "Statistics updated", category: "maintenance" };
};

const describeUnrecognizedChanges = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
): DescribedChange[] => {
  const ignoredPropertyChanges = describeIgnoredPropertyChanges(
    previousFile.properties,
    currentFile.properties,
  );
  const fieldTitles = diffMetadataFiles(previousFile, currentFile)
    .filter((diff) => diff.key !== "properties")
    .map((diff) => FIELD_TITLES[diff.key] ?? diff.key);
  const changedFields =
    fieldTitles.length === 0
      ? []
      : [sameTextChange(`Changed: ${fieldTitles.join(", ")}`)];
  const changes = [...ignoredPropertyChanges, ...changedFields];
  if (changes.length === 0) {
    return [sameTextChange("No change in the fields IceGraph reads")];
  }

  return changes;
};

export const describeMetadataOnlyCommit = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
  tableMetadata: TableMetadata,
  expiredSnapshotCount: number,
): CommitDescription => {
  const schemaChanges = describeSchemaChange(
    previousFile.current_schema_id,
    currentFile.current_schema_id,
    tableMetadata,
  );
  const partitioningChanges = describePartitionSpecChange(
    previousFile.partition_spec_id,
    currentFile.partition_spec_id,
    tableMetadata,
  );
  const sortOrderChanges = describeSortOrderChange(
    previousFile.sort_order_id,
    currentFile.sort_order_id,
    tableMetadata,
  );
  const refChanges = describeRefChanges(
    previousFile.refs,
    currentFile.refs,
    null,
  );
  const propertyChanges = describePropertyChanges(
    previousFile.properties,
    currentFile.properties,
    previousFile.sort_order_id !== currentFile.sort_order_id,
  );
  const statisticsChanges = describeStatisticsChanges(
    previousFile,
    currentFile,
  );
  const expiryChanges = describeExpiredSnapshots(expiredSnapshotCount);

  const pickHeadline = (): Headline => {
    if (schemaChanges.length > 0) {
      return { title: "Schema changed", category: "definition" };
    }
    if (partitioningChanges.length > 0) {
      return { title: "Partitioning changed", category: "definition" };
    }
    if (sortOrderChanges.length > 0) {
      return { title: "Sort order changed", category: "definition" };
    }
    if (refChanges.length > 0) {
      return {
        title: isTagChange(previousFile.refs, currentFile.refs)
          ? "Tag changed"
          : "Branch changed",
        category: "reference",
      };
    }
    if (propertyChanges.length > 0) {
      return { title: "Settings changed", category: "quiet" };
    }
    if (statisticsChanges.length > 0) {
      return statisticsHeadline(statisticsChanges);
    }
    if (expiryChanges.length > 0) {
      return { title: "History trimmed", category: "maintenance" };
    }
    return { title: UNRECOGNIZED_METADATA_TITLE, category: "quiet" };
  };

  const changes = [
    ...schemaChanges,
    ...partitioningChanges,
    ...sortOrderChanges,
    ...refChanges,
    ...propertyChanges,
    ...statisticsChanges,
    ...expiryChanges,
  ];
  const shownChanges =
    changes.length > 0
      ? changes
      : describeUnrecognizedChanges(previousFile, currentFile);

  return {
    kind: "metadata-only",
    ...pickHeadline(),
    impactSegments: shownChanges.map(
      (change) => change.impactSegment ?? impactText(change.impact),
    ),
    detailTexts: shownChanges.map((change) => change.detail),
    snapshotId: null,
    branchName: null,
    repointTargetId: null,
    movedToBranchName: null,
  };
};
