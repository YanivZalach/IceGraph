import type { TableMetadata } from "../../api/tableMetadataSchema";
import {
  formatId,
  sameTextChange,
  type DescribedChange,
} from "./describedChange";

type SortOrderField = NonNullable<
  TableMetadata["sort-orders"]
>[number]["fields"][number];

const findColumnName = (
  tableMetadata: TableMetadata,
  columnId: number,
): string | undefined =>
  [...(tableMetadata.schemas ?? [])]
    .reverse()
    .flatMap((schema) => schema.fields)
    .find((field) => field.id === columnId)?.name;

const describeSortField = (
  field: SortOrderField,
  tableMetadata: TableMetadata,
): string => {
  const columnName =
    findColumnName(tableMetadata, field["source-id"]) ??
    `field ${field["source-id"].toString()}`;
  const sortedExpression =
    field.transform === "identity"
      ? columnName
      : `${field.transform}(${columnName})`;
  const direction = field.direction ?? null;

  return direction === null
    ? sortedExpression
    : `${sortedExpression} ${direction}`;
};

export const describeSortOrderChange = (
  previousSortOrderId: number | null,
  currentSortOrderId: number | null,
  tableMetadata: TableMetadata,
): DescribedChange[] => {
  if (previousSortOrderId === currentSortOrderId) {
    return [];
  }

  const currentOrder = tableMetadata["sort-orders"]?.find(
    (order) => order["order-id"] === currentSortOrderId,
  );

  if (currentOrder === undefined) {
    return [
      sameTextChange(
        `sort order changed (${formatId(previousSortOrderId)} → ${formatId(currentSortOrderId)})`,
      ),
    ];
  }

  if (currentOrder.fields.length === 0) {
    return [sameTextChange("sort order removed")];
  }

  const sortedExpressions = currentOrder.fields
    .map((field) => describeSortField(field, tableMetadata))
    .join(", ");

  return [sameTextChange(`now sorted by ${sortedExpressions}`)];
};
