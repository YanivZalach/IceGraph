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
    if (expiryChanges.length > 0) {
      return { title: "History trimmed", category: "maintenance" };
    }
    return { title: "Metadata updated", category: "quiet" };
  };

  const changes = [
    ...schemaChanges,
    ...partitioningChanges,
    ...sortOrderChanges,
    ...refChanges,
    ...propertyChanges,
    ...expiryChanges,
  ];
  const shownChanges =
    changes.length > 0
      ? changes
      : describeIgnoredPropertyChanges(
          previousFile.properties,
          currentFile.properties,
        );

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
