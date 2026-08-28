import {
  compareBigInts,
  createMetricRatio,
  parseExactCount,
  parseExactSignedCount,
  type MetricRatio,
} from "./exactCounts";
import type { ReadableMetrics } from "./readableMetricValues";

export interface ReadableMetricTableRow {
  averageBytesPerValue: MetricRatio | undefined;
  columnName: string;
  columnSizeBytes: bigint | undefined;
  columnSizePercentage: MetricRatio | undefined;
  fieldType: string;
  lowerBound: unknown;
  nanPercentage: MetricRatio | undefined;
  nanValueCount: bigint | undefined;
  nullPercentage: MetricRatio | undefined;
  nullValueCount: bigint | undefined;
  sourceId: unknown;
  upperBound: unknown;
  valueCount: bigint | undefined;
}

export interface ReadableMetricsSummary {
  columnCount: number;
  totalMeasuredColumnSizeBytes: bigint;
}

type BoundKey = "lowerBound" | "upperBound";

const INTEGER_FIELD_TYPES = new Set(["int", "long"]);
const FLOAT_FIELD_TYPES = new Set(["float", "double"]);
const DATE_FIELD_TYPES = new Set([
  "date",
  "timestamp",
  "timestamptz",
  "timestamp_ns",
  "timestamptz_ns",
]);

export const buildReadableMetricRows = (
  readableMetrics: ReadableMetrics,
  totalFileSizeBytes: number | null,
): ReadableMetricTableRow[] => {
  const totalFileSize = parseExactCount(totalFileSizeBytes);

  return Object.entries(readableMetrics).map(([columnName, metrics]) => {
    const columnSizeBytes = parseExactCount(metrics.column_size_in_bytes);
    const valueCount = parseExactCount(metrics.value_count);
    const nullValueCount = parseExactCount(metrics.null_value_count);
    const nanValueCount = parseExactCount(metrics.nan_value_count);

    return {
      averageBytesPerValue: createMetricRatio(columnSizeBytes, valueCount),
      columnName,
      columnSizeBytes,
      columnSizePercentage: createMetricRatio(
        columnSizeBytes,
        totalFileSize,
        true,
      ),
      fieldType: metrics.field_type,
      lowerBound: metrics.lower_bound,
      nanPercentage: createMetricRatio(nanValueCount, valueCount, true),
      nanValueCount,
      nullPercentage: createMetricRatio(nullValueCount, valueCount, true),
      nullValueCount,
      sourceId: metrics.source_id,
      upperBound: metrics.upper_bound,
      valueCount,
    };
  });
};

export const summarizeReadableMetrics = (
  readableMetrics: ReadableMetrics,
): ReadableMetricsSummary => {
  const rows = buildReadableMetricRows(readableMetrics, null);
  return {
    columnCount: rows.length,
    totalMeasuredColumnSizeBytes: rows.reduce(
      (total, row) => total + (row.columnSizeBytes ?? 0n),
      0n,
    ),
  };
};

const normalizeSourceId = (value: unknown): bigint | string =>
  parseExactCount(value) ?? String(value);

export const compareSourceIds = (first: unknown, second: unknown): number => {
  const normalizedFirst = normalizeSourceId(first);
  const normalizedSecond = normalizeSourceId(second);
  if (
    typeof normalizedFirst === "bigint" &&
    typeof normalizedSecond === "bigint"
  ) {
    return compareBigInts(normalizedFirst, normalizedSecond);
  }
  if (typeof normalizedFirst !== typeof normalizedSecond) {
    return typeof normalizedFirst === "bigint" ? -1 : 1;
  }
  return String(normalizedFirst).localeCompare(
    String(normalizedSecond),
    undefined,
    { numeric: true },
  );
};

const compareNumbers = (first: number, second: number): number =>
  first === second ? 0 : first < second ? -1 : 1;

const parseFiniteNumber = (value: unknown): number | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string" || value.trim() === "") return undefined;
  const parsedValue = Number(value);
  return Number.isFinite(parsedValue) ? parsedValue : undefined;
};

interface ParsedDecimal {
  coefficient: bigint;
  scale: number;
}

const parseDecimal = (value: unknown): ParsedDecimal | undefined => {
  const text = typeof value === "string" ? value.trim() : String(value);
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(text);
  if (match === null) return undefined;
  const integer = match[2];
  if (integer === undefined) return undefined;
  const fraction = match[3] ?? "";
  const coefficient = BigInt(`${integer}${fraction}`);
  return {
    coefficient: match[1] === "-" ? -coefficient : coefficient,
    scale: fraction.length,
  };
};

const compareDecimals = (
  first: ParsedDecimal,
  second: ParsedDecimal,
): number => {
  const commonScale = Math.max(first.scale, second.scale);
  return compareBigInts(
    first.coefficient * 10n ** BigInt(commonScale - first.scale),
    second.coefficient * 10n ** BigInt(commonScale - second.scale),
  );
};

const formatBoundForComparison = (value: unknown): string => {
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

const compareBoundValues = (
  first: unknown,
  second: unknown,
  fieldType: string,
): number => {
  if (INTEGER_FIELD_TYPES.has(fieldType)) {
    const firstInteger = parseExactSignedCount(first);
    const secondInteger = parseExactSignedCount(second);
    if (firstInteger !== undefined && secondInteger !== undefined) {
      return compareBigInts(firstInteger, secondInteger);
    }
  }

  if (fieldType.startsWith("decimal")) {
    const firstDecimal = parseDecimal(first);
    const secondDecimal = parseDecimal(second);
    if (firstDecimal !== undefined && secondDecimal !== undefined) {
      return compareDecimals(firstDecimal, secondDecimal);
    }
  }

  if (FLOAT_FIELD_TYPES.has(fieldType)) {
    const firstNumber = parseFiniteNumber(first);
    const secondNumber = parseFiniteNumber(second);
    if (firstNumber !== undefined && secondNumber !== undefined) {
      return compareNumbers(firstNumber, secondNumber);
    }
  }

  if (DATE_FIELD_TYPES.has(fieldType)) {
    const firstTimestamp =
      typeof first === "string" ? Date.parse(first) : Number.NaN;
    const secondTimestamp =
      typeof second === "string" ? Date.parse(second) : Number.NaN;
    if (!Number.isNaN(firstTimestamp) && !Number.isNaN(secondTimestamp)) {
      return compareNumbers(firstTimestamp, secondTimestamp);
    }
  }

  if (typeof first === "boolean" && typeof second === "boolean") {
    return first === second ? 0 : first ? 1 : -1;
  }

  const firstText = formatBoundForComparison(first);
  const secondText = formatBoundForComparison(second);
  return firstText.localeCompare(secondText, undefined, { numeric: true });
};

export const compareReadableMetricBounds = (
  first: ReadableMetricTableRow,
  second: ReadableMetricTableRow,
  boundKey: BoundKey,
): number => {
  const fieldTypeComparison = first.fieldType.localeCompare(second.fieldType);
  if (fieldTypeComparison !== 0) return fieldTypeComparison;
  return compareBoundValues(first[boundKey], second[boundKey], first.fieldType);
};

const formatScaledInteger = (
  scaledValue: bigint,
  fractionDigits: number,
  trimTrailingZeros: boolean,
): string => {
  const scale = 10n ** BigInt(fractionDigits);
  const integerPart = scaledValue / scale;
  let fractionPart = (scaledValue % scale)
    .toString()
    .padStart(fractionDigits, "0");
  if (trimTrailingZeros) fractionPart = fractionPart.replace(/0+$/, "");
  return fractionPart === ""
    ? integerPart.toLocaleString()
    : `${integerPart.toLocaleString()}.${fractionPart}`;
};

const scaleRatio = (ratio: MetricRatio, scale: bigint): bigint =>
  (ratio.numerator * scale + ratio.denominator / 2n) / ratio.denominator;

export const formatMetricPercentage = (
  ratio: MetricRatio | undefined | null,
): string =>
  ratio == null
    ? "-"
    : `${formatScaledInteger(scaleRatio(ratio, 10_000n), 2, false)}%`;

export const formatAverageBytesPerValue = (
  ratio: MetricRatio | undefined | null,
): string =>
  ratio == null
    ? "-"
    : `${formatScaledInteger(scaleRatio(ratio, 100n), 2, true)} B`;

export const formatMetricInteger = (value: bigint | undefined): string =>
  value?.toLocaleString() ?? "-";
