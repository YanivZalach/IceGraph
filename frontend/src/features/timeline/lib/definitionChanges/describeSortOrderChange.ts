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

// v3 multi-argument transforms list their columns in `source-ids`.
const sourceColumnIds = (field: SortOrderField): number[] =>
  field["source-ids"] ??
  (field["source-id"] == null ? [] : [field["source-id"]]);

const describeSortField = (
  field: SortOrderField,
  tableMetadata: TableMetadata,
): string => {
  const columnNames = sourceColumnIds(field)
    .map(
      (columnId) =>
        findColumnName(tableMetadata, columnId) ??
        `field ${columnId.toString()}`,
    )
    .join(", ");
  const sortedExpression =
    field.transform === "identity"
      ? columnNames
      : `${field.transform}(${columnNames})`;
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
