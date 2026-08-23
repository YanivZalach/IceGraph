import type { MetadataFileNode } from "../../api/nodeSchemas";
import type { TableMetadata } from "../../api/tableMetadataSchema";
import type { DescribedChange } from "./describedChange";
import { describePartitionSpecChange } from "./describePartitionSpecChange";
import { describePropertyChanges } from "./describePropertyChanges";
import { describeRefChanges } from "./describeRefChanges";
import { describeSchemaChange } from "./describeSchemaChange";
import { describeSortOrderChange } from "./describeSortOrderChange";

export const listDefinitionChanges = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
  tableMetadata: TableMetadata,
  rowSnapshotId: string | null,
): DescribedChange[] => [
  ...describeSchemaChange(
    previousFile.current_schema_id,
    currentFile.current_schema_id,
    tableMetadata,
  ),
  ...describePartitionSpecChange(
    previousFile.partition_spec_id,
    currentFile.partition_spec_id,
    tableMetadata,
  ),
  ...describeSortOrderChange(
    previousFile.sort_order_id,
    currentFile.sort_order_id,
    tableMetadata,
  ),
  ...describeRefChanges(previousFile.refs, currentFile.refs, rowSnapshotId),
  ...describePropertyChanges(previousFile.properties, currentFile.properties),
];
