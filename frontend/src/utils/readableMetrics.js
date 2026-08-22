import { formatLocaleDateTime, parseUtcDate } from "./dateUtils.js";

const LOCAL_TIMESTAMP_FIELD_TYPES = new Set([
  "timestamp",
  "timestamptz",
  "timestamp_ns",
  "timestamptz_ns",
]);

export const formatReadableMetricValue = (value, metricName, fieldType) => {
  if (value == null || value === "") return "-";
  if (metricName === "column_size_mib") {
    const numericValue = Number(value);
    if (Number.isFinite(numericValue)) {
      return numericValue.toLocaleString("en-US", {
        maximumFractionDigits: 20,
        useGrouping: false,
      });
    }
  }
  if (
    typeof value === "string" &&
    ["lower_bound", "upper_bound"].includes(metricName) &&
    LOCAL_TIMESTAMP_FIELD_TYPES.has(fieldType)
  ) {
    const date = parseUtcDate(value);
    if (date) return formatLocaleDateTime(date);
  }
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};
