import {
  createColumnHelper,
  createSortedRowModel,
  rowSortingFeature,
  tableFeatures,
  useTable,
} from "@tanstack/react-table";
import type { z } from "zod";
import { PanelSectionTitle } from "../../../components/PanelContent";
import { UI_HELPER_TEXT_CLASS } from "../../../uiTypography";
import { compareBigInts } from "../../../utils/readableMetricsStatistics";
import { formatLocaleDateTime } from "../../../utils/dateUtils";
import type { partitionStatisticsRowSchema } from "../api/graphSchemas";

type PartitionStatisticsRow = z.infer<typeof partitionStatisticsRowSchema>;

interface PartitionStatisticsTableProps {
  partitionsCount: number | string | null;
  sampledPartitions: PartitionStatisticsRow[];
}

const PARTITION_COLUMN = "partition";
const LAST_UPDATED_AT_COLUMN = "last_updated_at";

const PARTITION_TABLE_FEATURES = tableFeatures({
  rowSortingFeature,
  sortedRowModel: createSortedRowModel(),
});
const columnHelper = createColumnHelper<
  typeof PARTITION_TABLE_FEATURES,
  PartitionStatisticsRow
>();

const toBigInt = (value: unknown): bigint | null => {
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return BigInt(value);
  }
  if (typeof value === "string" && /^-?\d+$/.test(value)) return BigInt(value);
  return null;
};

const toDisplayString = (value: unknown): string => {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean"
  ) {
    return String(value);
  }
  return JSON.stringify(value);
};

const comparePartitionValues = (first: unknown, second: unknown): number => {
  const firstNumber = toBigInt(first);
  const secondNumber = toBigInt(second);
  if (firstNumber !== null && secondNumber !== null) {
    return compareBigInts(firstNumber, secondNumber);
  }
  return toDisplayString(first).localeCompare(toDisplayString(second));
};

const formatPartitionValue = (columnId: string, value: unknown): string => {
  if (columnId === LAST_UPDATED_AT_COLUMN && typeof value === "number") {
    return formatLocaleDateTime(new Date(value));
  }
  return toDisplayString(value);
};

const buildColumns = (sampledPartitions: PartitionStatisticsRow[]) =>
  [...new Set(sampledPartitions.flatMap((row) => Object.keys(row)))].map(
    (columnId) =>
      columnHelper.accessor((row) => row[columnId], {
        cell: ({ getValue }) => formatPartitionValue(columnId, getValue()),
        header: columnId,
        id: columnId,
        sortFn: (first, second) =>
          comparePartitionValues(
            first.original[columnId],
            second.original[columnId],
          ),
        sortUndefined: "last",
      }),
  );

const PartitionStatisticsTable = ({
  partitionsCount,
  sampledPartitions,
}: PartitionStatisticsTableProps) => {
  const table = useTable({
    columns: buildColumns(sampledPartitions),
    data: sampledPartitions,
    enableMultiSort: false,
    enableSortingRemoval: true,
    features: PARTITION_TABLE_FEATURES,
    getRowId: (_row, index) => String(index),
  });
  if (sampledPartitions.length === 0) return null;
  const isSampledByLastUpdate = sampledPartitions.some(
    (row) => LAST_UPDATED_AT_COLUMN in row,
  );

  return (
    <div>
      <PanelSectionTitle>Partitions</PanelSectionTitle>
      <p className={`${UI_HELPER_TEXT_CLASS} mb-2`}>
        Showing {sampledPartitions.length} of{" "}
        {partitionsCount ?? sampledPartitions.length} partitions
        {isSampledByLastUpdate ? ", sampled by most recent update" : ""}
      </p>
      <div className="overflow-x-auto rounded-lg border border-edge">
        <table className="w-full text-left font-mono text-xs text-slate-200">
          <thead className="bg-surface text-slate-400">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const sortDirection = header.column.getIsSorted();
                  const nextSortDirection = header.column.getNextSortingOrder();
                  const nextSortLabel =
                    nextSortDirection === false
                      ? "restore original order"
                      : `sort ${nextSortDirection === "asc" ? "ascending" : "descending"}`;

                  return (
                    <th
                      key={header.id}
                      aria-sort={
                        sortDirection === "asc"
                          ? "ascending"
                          : sortDirection === "desc"
                            ? "descending"
                            : "none"
                      }
                      className="whitespace-nowrap px-3 py-2 font-semibold"
                    >
                      <button
                        type="button"
                        aria-label={`${header.column.id}, ${nextSortLabel}`}
                        onClick={header.column.getToggleSortingHandler()}
                        className="group flex cursor-pointer items-center gap-1 rounded text-left hover:text-ink focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent"
                      >
                        <table.FlexRender header={header} />
                        <span
                          aria-hidden="true"
                          className={
                            sortDirection === false
                              ? "text-slate-600 group-hover:text-slate-400"
                              : "text-accent"
                          }
                        >
                          {sortDirection === "asc"
                            ? "↑"
                            : sortDirection === "desc"
                              ? "↓"
                              : "↕"}
                        </span>
                      </button>
                    </th>
                  );
                })}
              </tr>
            ))}
          </thead>
          <tbody className="divide-y divide-edge">
            {table.getRowModel().rows.map((row) => (
              <tr key={row.id} className="bg-canvas align-top">
                {row.getAllCells().map((cell) =>
                  cell.column.id === PARTITION_COLUMN ? (
                    <th
                      key={cell.id}
                      scope="row"
                      className="whitespace-nowrap px-3 py-2 font-semibold text-accent"
                    >
                      <table.FlexRender cell={cell} />
                    </th>
                  ) : (
                    <td key={cell.id} className="whitespace-nowrap px-3 py-2">
                      <table.FlexRender cell={cell} />
                    </td>
                  ),
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default PartitionStatisticsTable;
