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

export const formatPlainNumber = (value: number): string => {
  const text = value.toString();
  const exponentMatch = /^(-?)(\d+)(?:\.(\d+))?e([+-]\d+)$/.exec(text);
  if (exponentMatch === null) return text;
  const [, sign = "", integerDigits = "", fractionDigits = "", exponent = "0"] =
    exponentMatch;
  const digits = `${integerDigits}${fractionDigits}`;
  const pointIndex = integerDigits.length + Number(exponent);
  if (pointIndex <= 0) return `${sign}0.${"0".repeat(-pointIndex)}${digits}`;
  if (pointIndex >= digits.length) {
    return `${sign}${digits}${"0".repeat(pointIndex - digits.length)}`;
  }
  return `${sign}${digits.slice(0, pointIndex)}.${digits.slice(pointIndex)}`;
};

const formatNestedMetricValue = (value: unknown): string => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? formatPlainNumber(value) : "null";
  }
  if (Array.isArray(value)) {
    return `[${value.map((item: unknown) => formatNestedMetricValue(item)).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value)
      .filter(([, item]) => item !== undefined)
      .map(
        ([key, item]) =>
          `${JSON.stringify(key)}:${formatNestedMetricValue(item)}`,
      )
      .join(",")}}`;
  }
  if (value === undefined) return "null";
  return JSON.stringify(value);
};

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

  if (typeof value === "object") return formatNestedMetricValue(value);
  if (typeof value === "string") return value;
  if (typeof value === "number") return formatPlainNumber(value);
  if (typeof value === "boolean" || typeof value === "bigint") {
    return value.toString();
  }

  return "-";
};
