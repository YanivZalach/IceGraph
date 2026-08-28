import {
  createColumnHelper,
  createSortedRowModel,
  rowSortingFeature,
  tableFeatures,
} from "@tanstack/react-table";
import { formatBytesAsMebibytes } from "../lib/formatBytes";
import { formatReadableMetricValue } from "./readableMetricValues";
import {
  compareBigInts,
  compareMetricRatios,
  type MetricRatio,
} from "./exactCounts";
import {
  compareReadableMetricBounds,
  compareSourceIds,
  formatAverageBytesPerValue,
  formatMetricInteger,
  formatMetricPercentage,
  type ReadableMetricTableRow,
} from "./metricRows";

export const METRICS_TABLE_FEATURES = tableFeatures({
  rowSortingFeature,
  sortedRowModel: createSortedRowModel(),
});
const columnHelper = createColumnHelper<
  typeof METRICS_TABLE_FEATURES,
  ReadableMetricTableRow
>();

type BigIntKey =
  "columnSizeBytes" | "nanValueCount" | "nullValueCount" | "valueCount";
type RatioKey =
  | "averageBytesPerValue"
  | "columnSizePercentage"
  | "nanPercentage"
  | "nullPercentage";
type BoundKey = "lowerBound" | "upperBound";

const isMissingMetric = (value: unknown): boolean =>
  value === null || value === undefined || value === "";

const bigIntColumn = (
  key: BigIntKey,
  header: string,
  formatValue = formatMetricInteger,
) =>
  columnHelper.accessor((row) => row[key], {
    cell: ({ getValue }) => formatValue(getValue()),
    header,
    id: key,
    sortDescFirst: false,
    sortFn: (first, second) =>
      compareBigInts(first.original[key] ?? 0n, second.original[key] ?? 0n),
    sortUndefined: "last",
  });

const ratioColumn = (
  key: RatioKey,
  header: string,
  formatValue: (
    ratio: MetricRatio | undefined,
  ) => string = formatMetricPercentage,
) =>
  columnHelper.accessor((row) => row[key], {
    cell: ({ getValue }) => formatValue(getValue()),
    header,
    id: key,
    sortDescFirst: false,
    sortFn: (first, second) =>
      compareMetricRatios(
        first.original[key] ?? { denominator: 1n, numerator: 0n },
        second.original[key] ?? { denominator: 1n, numerator: 0n },
      ),
    sortUndefined: "last",
  });

const boundColumn = (key: BoundKey, header: string) =>
  columnHelper.accessor(
    (row) => (isMissingMetric(row[key]) ? undefined : row[key]),
    {
      cell: ({ row }) =>
        formatReadableMetricValue(
          row.original[key],
          key === "lowerBound" ? "lower_bound" : "upper_bound",
          row.original.fieldType,
        ),
      header,
      id: key,
      sortDescFirst: false,
      sortFn: (first, second) =>
        compareReadableMetricBounds(first.original, second.original, key),
      sortUndefined: "last",
    },
  );

const createColumns = (sizeScope: "file" | "files") =>
  columnHelper.columns([
    columnHelper.accessor("columnName", {
      cell: ({ getValue }) => getValue(),
      header: "column",
      sortDescFirst: false,
      sortFn: (first, second) =>
        first.original.columnName.localeCompare(
          second.original.columnName,
          undefined,
          { numeric: true },
        ),
    }),
    columnHelper.accessor(
      (row) => (isMissingMetric(row.sourceId) ? undefined : row.sourceId),
      {
        cell: ({ row }) =>
          formatReadableMetricValue(
            row.original.sourceId,
            "source_id",
            row.original.fieldType,
          ),
        header: "source ID",
        id: "sourceId",
        sortDescFirst: false,
        sortFn: (first, second) =>
          compareSourceIds(first.original.sourceId, second.original.sourceId),
        sortUndefined: "last",
      },
    ),
    columnHelper.accessor("fieldType", {
      cell: ({ getValue }) => getValue(),
      header: "field type",
      sortDescFirst: false,
      sortFn: (first, second) =>
        first.original.fieldType.localeCompare(second.original.fieldType),
    }),
    bigIntColumn("columnSizeBytes", "column size", (value) =>
      value === undefined ? "-" : formatBytesAsMebibytes(value.toString()),
    ),
    ratioColumn("columnSizePercentage", `column size % of ${sizeScope}`),
    bigIntColumn("valueCount", "value count"),
    bigIntColumn("nullValueCount", "null count"),
    ratioColumn("nullPercentage", "null %"),
    bigIntColumn("nanValueCount", "NaN count"),
    ratioColumn("nanPercentage", "NaN %"),
    ratioColumn(
      "averageBytesPerValue",
      "average bytes / value",
      formatAverageBytesPerValue,
    ),
    boundColumn("lowerBound", "lower bound"),
    boundColumn("upperBound", "upper bound"),
  ]);

const FILE_COLUMNS = createColumns("file");
const FILES_COLUMNS = createColumns("files");

export const getReadableMetricsColumns = (sizeScope: "file" | "files") =>
  sizeScope === "file" ? FILE_COLUMNS : FILES_COLUMNS;
