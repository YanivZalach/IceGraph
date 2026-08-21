import type { TableMetadata } from "../../api/tableMetadataSchema";
import {
  formatId,
  sameTextChange,
  type DescribedChange,
} from "./describedChange";

export const describePartitionSpecChange = (
  previousSpecId: number | null,
  currentSpecId: number | null,
  tableMetadata: TableMetadata,
): DescribedChange[] => {
  if (previousSpecId === currentSpecId) {
    return [];
  }

  const currentSpec = tableMetadata["partition-specs"]?.find(
    (spec) => spec["spec-id"] === currentSpecId,
  );

  if (currentSpec === undefined) {
    return [
      sameTextChange(
        `partitioning changed (${formatId(previousSpecId)} → ${formatId(currentSpecId)})`,
      ),
    ];
  }

  if (currentSpec.fields.length === 0) {
    return [sameTextChange("no longer partitioned")];
  }

  const fieldNames = currentSpec.fields.map((field) => field.name).join(", ");

  return [sameTextChange(`now partitioned by ${fieldNames}`)];
};
