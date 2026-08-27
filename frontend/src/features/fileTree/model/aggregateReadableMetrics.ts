import type {
  ReadableColumnMetrics,
  ReadableMetrics,
} from "../../../utils/readableMetrics";
import type { DataFileNode } from "../types";

const NUMERIC_BOUND_FIELD_TYPES = new Set(["int", "long", "float", "double"]);
const STRING_BOUND_FIELD_TYPES = new Set([
  "string",
  "date",
  "time",
  "timestamp",
  "timestamptz",
  "timestamp_ns",
  "timestamptz_ns",
  "uuid",
]);

interface BoundAccumulator {
  isComparable: boolean;
  value: unknown;
}

interface MutableColumnMetrics {
  columnSizeInBytes: bigint | null;
  fieldType: string;
  isCompatible: boolean;
  lowerBound: BoundAccumulator;
  nanValueCount: bigint | null;
  nullValueCount: bigint | null;
  sourceId: string | number;
  upperBound: BoundAccumulator;
  valueCount: bigint | null;
}

const addCountMetric = (
  currentTotal: bigint | null,
  value: unknown,
): bigint | null => {
  const count =
    typeof value === "bigint" && value >= 0n
      ? value
      : typeof value === "number" && Number.isSafeInteger(value) && value >= 0
        ? BigInt(value)
        : typeof value === "string" && /^\d+$/.test(value)
          ? BigInt(value)
          : null;
  return count === null ? currentTotal : (currentTotal ?? 0n) + count;
};

const addByteMetric = (
  currentTotal: bigint | null,
  value: unknown,
): bigint | null => {
  if (typeof value !== "string" || !/^\d+$/.test(value)) return currentTotal;
  return (currentTotal ?? 0n) + BigInt(value);
};

const compareBoundValues = (
  firstValue: unknown,
  secondValue: unknown,
  fieldType: string,
): number | null => {
  if (
    NUMERIC_BOUND_FIELD_TYPES.has(fieldType) &&
    typeof firstValue === "number" &&
    Number.isFinite(firstValue) &&
    typeof secondValue === "number" &&
    Number.isFinite(secondValue)
  ) {
    return firstValue === secondValue ? 0 : firstValue < secondValue ? -1 : 1;
  }
  if (
    STRING_BOUND_FIELD_TYPES.has(fieldType) &&
    typeof firstValue === "string" &&
    typeof secondValue === "string"
  ) {
    return firstValue === secondValue ? 0 : firstValue < secondValue ? -1 : 1;
  }
  if (
    fieldType === "boolean" &&
    typeof firstValue === "boolean" &&
    typeof secondValue === "boolean"
  ) {
    return firstValue === secondValue ? 0 : firstValue ? 1 : -1;
  }
  return null;
};

const accumulateBound = (
  accumulator: BoundAccumulator,
  candidateValue: unknown,
  fieldType: string,
  shouldSelectLowerValue: boolean,
): BoundAccumulator => {
  if (candidateValue === null || candidateValue === undefined) {
    return accumulator;
  }
  if (!accumulator.isComparable) return accumulator;
  if (accumulator.value === null) {
    const isComparable =
      compareBoundValues(candidateValue, candidateValue, fieldType) !== null;
    return {
      isComparable,
      value: isComparable ? candidateValue : null,
    };
  }

  const comparison = compareBoundValues(
    candidateValue,
    accumulator.value,
    fieldType,
  );
  if (comparison === null) return { isComparable: false, value: null };
  const shouldReplace = shouldSelectLowerValue
    ? comparison < 0
    : comparison > 0;
  return {
    isComparable: true,
    value: shouldReplace ? candidateValue : accumulator.value,
  };
};

const createColumnMetrics = (
  metrics: ReadableColumnMetrics,
): MutableColumnMetrics => ({
  columnSizeInBytes: null,
  fieldType: metrics.field_type,
  isCompatible: true,
  lowerBound: { isComparable: true, value: null },
  nanValueCount: null,
  nullValueCount: null,
  sourceId:
    typeof metrics.source_id === "string" ||
    typeof metrics.source_id === "number"
      ? metrics.source_id
      : "",
  upperBound: { isComparable: true, value: null },
  valueCount: null,
});

const accumulateColumnMetrics = (
  aggregate: MutableColumnMetrics,
  metrics: ReadableColumnMetrics,
) => {
  if (
    metrics.field_type !== aggregate.fieldType ||
    metrics.source_id !== aggregate.sourceId
  ) {
    aggregate.isCompatible = false;
    return;
  }

  aggregate.columnSizeInBytes = addByteMetric(
    aggregate.columnSizeInBytes,
    metrics.column_size_in_bytes,
  );
  aggregate.valueCount = addCountMetric(
    aggregate.valueCount,
    metrics.value_count,
  );
  aggregate.nullValueCount = addCountMetric(
    aggregate.nullValueCount,
    metrics.null_value_count,
  );
  aggregate.nanValueCount = addCountMetric(
    aggregate.nanValueCount,
    metrics.nan_value_count,
  );
  aggregate.lowerBound = accumulateBound(
    aggregate.lowerBound,
    metrics.lower_bound,
    aggregate.fieldType,
    true,
  );
  aggregate.upperBound = accumulateBound(
    aggregate.upperBound,
    metrics.upper_bound,
    aggregate.fieldType,
    false,
  );
};

export const aggregateReadableMetrics = (
  files: DataFileNode[],
): ReadableMetrics => {
  const aggregatesByColumnName = new Map<string, MutableColumnMetrics>();
  for (const file of files) {
    for (const [columnName, metrics] of Object.entries(
      file.details.readable_metrics ?? {},
    )) {
      const aggregate =
        aggregatesByColumnName.get(columnName) ?? createColumnMetrics(metrics);
      accumulateColumnMetrics(aggregate, metrics);
      aggregatesByColumnName.set(columnName, aggregate);
    }
  }

  const readableMetrics: ReadableMetrics = {};
  for (const [columnName, aggregate] of aggregatesByColumnName) {
    if (!aggregate.isCompatible) continue;
    readableMetrics[columnName] = {
      source_id: aggregate.sourceId,
      field_type: aggregate.fieldType,
      column_size_in_bytes: aggregate.columnSizeInBytes?.toString() ?? null,
      value_count: aggregate.valueCount?.toString() ?? null,
      null_value_count: aggregate.nullValueCount?.toString() ?? null,
      nan_value_count: aggregate.nanValueCount?.toString() ?? null,
      lower_bound: aggregate.lowerBound.isComparable
        ? aggregate.lowerBound.value
        : null,
      upper_bound: aggregate.upperBound.isComparable
        ? aggregate.upperBound.value
        : null,
    };
  }
  return readableMetrics;
};
