import type { MetadataFileNode, SnapshotRefs } from "../../api/nodeSchemas";
import type { TableMetadata } from "../../api/tableMetadataSchema";
import type { CommitDescription } from "./commitDescription";
import { describePartitionSpecChange } from "../definitionChanges/describePartitionSpecChange";
import { describePropertyChanges } from "../definitionChanges/describePropertyChanges";
import { describeRefChanges } from "../definitionChanges/describeRefChanges";
import { describeSchemaChange } from "../definitionChanges/describeSchemaChange";
import { describeSortOrderChange } from "../definitionChanges/describeSortOrderChange";

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
  const refChanges = describeRefChanges(previousFile.refs, currentFile.refs);
  const propertyChanges = describePropertyChanges(
    previousFile.properties,
    currentFile.properties,
  );

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
    return "Metadata updated";
  };

  return {
    kind: "metadata-only",
    title: pickTitle(),
    impactSegments: [
      ...schemaChanges,
      ...partitioningChanges,
      ...sortOrderChanges,
      ...refChanges,
      ...propertyChanges,
    ].map((change) => change.impact),
    snapshotId: null,
    branchName: null,
    repointTargetId: null,
  };
};
