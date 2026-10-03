import {
  createSortedRowModel,
  rowSortingFeature,
  tableFeatures,
} from "@tanstack/react-table";

export const STATISTICS_TABLE_FEATURES = tableFeatures({
  rowSortingFeature,
  sortedRowModel: createSortedRowModel(),
});

export const parseSignedInteger = (value: unknown): bigint | undefined => {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return BigInt(value);
  }
  if (typeof value === "string" && /^-?\d+$/.test(value)) {
    return BigInt(value);
  }
  return undefined;
};

export const formatValue = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value.toString();
  }
  if (value === null || value === undefined) return "";
  return JSON.stringify(value);
};
