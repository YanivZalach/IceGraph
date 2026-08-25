import type { ReadableMetrics } from "./readableMetrics";

export interface ReadableColumnStatistics {
  averageBytesPerValue: number | null;
  columnName: string;
  columnSizeBytes: bigint | null;
  columnSizePercentage: number | null;
  nanPercentage: number | null;
  nullPercentage: number | null;
}

interface RankedNumberStatistic {
  columnNames: string[];
  value: number;
}

interface RankedSizeStatistic {
  columnNames: string[];
  value: bigint;
}

export interface ReadableMetricsSummary {
  highestNanPercentage: RankedNumberStatistic | null;
  highestNullPercentage: RankedNumberStatistic | null;
  largestColumns: RankedSizeStatistic | null;
  lowestNullPercentage: RankedNumberStatistic | null;
  measuredColumnCount: number;
  smallestColumns: RankedSizeStatistic | null;
  totalMeasuredColumnSizeBytes: bigint;
}

const parseByteCount = (value: unknown): bigint | null =>
  typeof value === "string" && /^\d+$/.test(value) ? BigInt(value) : null;

const parseMetricCount = (value: unknown): number | null =>
  typeof value === "number" && Number.isFinite(value) && value >= 0
    ? value
    : null;

const calculatePercentage = (
  numerator: number | null,
  denominator: number | null,
): number | null => {
  if (
    numerator === null ||
    denominator === null ||
    denominator <= 0 ||
    numerator > denominator
  ) {
    return null;
  }
  return (numerator / denominator) * 100;
};

const calculateSizePercentage = (
  columnSizeBytes: bigint | null,
  totalSizeBytes: bigint,
): number | null => {
  if (columnSizeBytes === null || totalSizeBytes === 0n) return null;
  const roundedBasisPoints =
    (columnSizeBytes * 10_000n + totalSizeBytes / 2n) / totalSizeBytes;
  return Number(roundedBasisPoints) / 100;
};

export const calculateReadableColumnStatistics = (
  readableMetrics: ReadableMetrics,
): ReadableColumnStatistics[] => {
  const columns = Object.entries(readableMetrics).map(
    ([columnName, metrics]) => ({
      columnName,
      columnSizeBytes: parseByteCount(metrics.column_size_in_bytes),
      metrics,
    }),
  );
  const totalMeasuredColumnSizeBytes = columns.reduce(
    (total, column) => total + (column.columnSizeBytes ?? 0n),
    0n,
  );

  return columns.map(({ columnName, columnSizeBytes, metrics }) => {
    const valueCount = parseMetricCount(metrics.value_count);
    const nullPercentage = calculatePercentage(
      parseMetricCount(metrics.null_value_count),
      valueCount,
    );
    const nanPercentage = calculatePercentage(
      parseMetricCount(metrics.nan_value_count),
      valueCount,
    );
    const numericColumnSize =
      columnSizeBytes === null ? null : Number(columnSizeBytes);

    return {
      averageBytesPerValue:
        numericColumnSize === null ||
        !Number.isFinite(numericColumnSize) ||
        valueCount === null ||
        valueCount <= 0
          ? null
          : numericColumnSize / valueCount,
      columnName,
      columnSizeBytes,
      columnSizePercentage: calculateSizePercentage(
        columnSizeBytes,
        totalMeasuredColumnSizeBytes,
      ),
      nanPercentage,
      nullPercentage,
    };
  });
};

const rankNumberStatistic = (
  columns: ReadableColumnStatistics[],
  getValue: (column: ReadableColumnStatistics) => number | null,
  direction: "highest" | "lowest",
): RankedNumberStatistic | null => {
  const rankedColumns = columns
    .map((column) => ({
      columnName: column.columnName,
      value: getValue(column),
    }))
    .filter(
      (column): column is { columnName: string; value: number } =>
        column.value !== null,
    );
  if (rankedColumns.length === 0) return null;
  const rankedValue = rankedColumns.reduce(
    (currentValue, column) =>
      direction === "highest"
        ? Math.max(currentValue, column.value)
        : Math.min(currentValue, column.value),
    rankedColumns[0]?.value ?? 0,
  );
  return {
    columnNames: rankedColumns
      .filter((column) => column.value === rankedValue)
      .map((column) => column.columnName),
    value: rankedValue,
  };
};

const rankSizeStatistic = (
  columns: ReadableColumnStatistics[],
  direction: "largest" | "smallest",
): RankedSizeStatistic | null => {
  const measuredColumns = columns.filter(
    (
      column,
    ): column is ReadableColumnStatistics & { columnSizeBytes: bigint } =>
      column.columnSizeBytes !== null,
  );
  if (measuredColumns.length === 0) return null;
  const rankedValue = measuredColumns.reduce((currentValue, column) => {
    const shouldReplace =
      direction === "largest"
        ? column.columnSizeBytes > currentValue
        : column.columnSizeBytes < currentValue;
    return shouldReplace ? column.columnSizeBytes : currentValue;
  }, measuredColumns[0]?.columnSizeBytes ?? 0n);
  return {
    columnNames: measuredColumns
      .filter((column) => column.columnSizeBytes === rankedValue)
      .map((column) => column.columnName),
    value: rankedValue,
  };
};

export const summarizeReadableMetrics = (
  readableMetrics: ReadableMetrics,
): ReadableMetricsSummary => {
  const columns = calculateReadableColumnStatistics(readableMetrics);
  const measuredColumns = columns.filter(
    (column) => column.columnSizeBytes !== null,
  );
  const highestNanPercentage = rankNumberStatistic(
    columns,
    (column) => column.nanPercentage,
    "highest",
  );

  return {
    highestNanPercentage:
      highestNanPercentage !== null && highestNanPercentage.value > 0
        ? highestNanPercentage
        : null,
    highestNullPercentage: rankNumberStatistic(
      columns,
      (column) => column.nullPercentage,
      "highest",
    ),
    largestColumns: rankSizeStatistic(columns, "largest"),
    lowestNullPercentage: rankNumberStatistic(
      columns,
      (column) => column.nullPercentage,
      "lowest",
    ),
    measuredColumnCount: measuredColumns.length,
    smallestColumns: rankSizeStatistic(columns, "smallest"),
    totalMeasuredColumnSizeBytes: measuredColumns.reduce(
      (total, column) => total + (column.columnSizeBytes ?? 0n),
      0n,
    ),
  };
};

export const formatMetricPercentage = (percentage: number | null): string =>
  percentage === null ? "-" : `${percentage.toFixed(2)}%`;

export const formatAverageBytesPerValue = (
  averageBytesPerValue: number | null,
): string =>
  averageBytesPerValue === null
    ? "-"
    : `${averageBytesPerValue.toLocaleString(undefined, {
        maximumFractionDigits: 2,
      })} B`;
