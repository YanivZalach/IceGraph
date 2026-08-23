import type { MetadataFileNode, SnapshotRefs } from "../../api/nodeSchemas";
import type { TableMetadata } from "../../api/tableMetadataSchema";
import type { CommitDescription } from "./commitDescription";
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
        previousRefs[name]?.["snapshot-id"] !== ref["snapshot-id"],
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
  );
  const expiryChanges = describeExpiredSnapshots(expiredSnapshotCount);

  const pickTitle = (): string => {
    if (schemaChanges.length > 0) {
      return "Schema changed";
    }
    if (partitioningChanges.length > 0) {
      return "Partitioning changed";
    }
    if (sortOrderChanges.length > 0) {
      return "Sort order changed";
    }
    if (refChanges.length > 0) {
      return isTagChange(previousFile.refs, currentFile.refs)
        ? "Tag changed"
        : "Branch changed";
    }
    if (propertyChanges.length > 0) {
      return "Settings changed";
    }
    if (expiryChanges.length > 0) {
      return "History trimmed";
    }
    return "Metadata updated";
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
    title: pickTitle(),
    impactSegments: shownChanges.map((change) => impactText(change.impact)),
    snapshotId: null,
    branchName: null,
    repointTargetId: null,
  };
};
