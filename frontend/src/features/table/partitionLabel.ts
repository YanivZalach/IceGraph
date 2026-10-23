import type { Partition } from "./api/graphSchemas";

export const ROOT_PARTITION_LABEL = "Root";

export const formatPartitionFieldLabel = (
  partitionField: Partition[number],
): string => `${partitionField.name} = ${partitionField.formatted_value}`;

export const formatPartitionLabel = (partition: Partition): string =>
  partition.length === 0
    ? ROOT_PARTITION_LABEL
    : partition.map(formatPartitionFieldLabel).join(", ");
