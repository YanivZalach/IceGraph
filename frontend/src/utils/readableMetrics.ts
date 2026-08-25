import { formatLocaleDateTime, parseUtcDate } from "./dateUtils.js";
import { formatBytesAsMebibytes } from "../shared/lib/formatBytes";

const LOCAL_TIMESTAMP_FIELD_TYPES = new Set<string>([
  "timestamp",
  "timestamptz",
  "timestamp_ns",
  "timestamptz_ns",
]);

export interface ReadableColumnMetrics {
  field_type: string;
  [metricName: string]: unknown;
}

export interface ReadableMetrics {
  [columnName: string]: ReadableColumnMetrics;
}

export const formatReadableMetricValue = (
  value: unknown,
  metricName: string,
  fieldType: string,
): string => {
  if (value == null || value === "") return "-";

  if (metricName === "column_size_in_bytes" && typeof value === "string") {
    return formatBytesAsMebibytes(value);
  }

  if (
    typeof value === "string" &&
    (metricName === "lower_bound" || metricName === "upper_bound") &&
    LOCAL_TIMESTAMP_FIELD_TYPES.has(fieldType)
  ) {
    const date = parseUtcDate(value);
    if (date) return formatLocaleDateTime(date);
  }

  if (typeof value === "object") return JSON.stringify(value);
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  ) {
    return value.toString();
  }

  return "-";
};
