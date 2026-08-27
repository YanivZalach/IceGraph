import { useTable } from "@tanstack/react-table";
import type { ReadableMetrics } from "../utils/readableMetrics";
import { buildReadableMetricRows } from "../utils/readableMetricsStatistics";
import { PanelSectionTitle } from "./PanelContent";
import {
  getReadableMetricsColumns,
  METRICS_TABLE_FEATURES,
} from "./readableMetricsTableColumns";

interface DataFileReadableMetricsTableProps {
  readableMetrics: ReadableMetrics;
  sizeScope: "file" | "files";
  title?: string;
  totalFileSizeBytes: number | null;
}

const DataFileReadableMetricsTable = ({
  readableMetrics,
  sizeScope,
  title = "Readable Metrics",
  totalFileSizeBytes,
}: DataFileReadableMetricsTableProps) => {
  const rows = buildReadableMetricRows(readableMetrics, totalFileSizeBytes);
  const table = useTable({
    columns: getReadableMetricsColumns(sizeScope),
    data: rows,
    enableMultiSort: false,
    enableSortingRemoval: true,
    features: METRICS_TABLE_FEATURES,
    getRowId: (row) => row.columnName,
    sortDescFirst: false,
  });
  if (rows.length === 0) return null;

  return (
    <div>
      <PanelSectionTitle>{title}</PanelSectionTitle>
      <div className="overflow-x-auto rounded-lg border border-edge">
        <table className="w-full text-left font-mono text-xs text-slate-200">
          <thead className="bg-surface text-slate-400">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const sortDirection = header.column.getIsSorted();
                  const nextSortDirection = header.column.getNextSortingOrder();
                  const headerLabel =
                    typeof header.column.columnDef.header === "string"
                      ? header.column.columnDef.header
                      : header.column.id;
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
                        aria-label={`${headerLabel}, ${nextSortLabel}`}
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
                  cell.column.id === "columnName" ? (
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

export default DataFileReadableMetricsTable;
