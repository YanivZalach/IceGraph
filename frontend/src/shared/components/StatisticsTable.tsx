import {
  useTable,
  type RowData,
  type TableOptions,
} from "@tanstack/react-table";
import type { ReactNode } from "react";
import { PanelSectionTitle } from "../../components/PanelContent";
import { STATISTICS_TABLE_FEATURES } from "./statisticsTableBase";

interface StatisticsTableProps<TData extends RowData> {
  columns: TableOptions<typeof STATISTICS_TABLE_FEATURES, TData>["columns"];
  description?: ReactNode;
  getRowId: (row: TData, index: number) => string;
  rowHeaderColumnId: string;
  rows: TData[];
  sortDescFirst?: boolean;
  title: string;
}

const StatisticsTable = <TData extends RowData>({
  columns,
  description,
  getRowId,
  rowHeaderColumnId,
  rows,
  sortDescFirst,
  title,
}: StatisticsTableProps<TData>) => {
  const table = useTable({
    columns,
    data: rows,
    enableMultiSort: false,
    enableSortingRemoval: true,
    features: STATISTICS_TABLE_FEATURES,
    getRowId,
    ...(sortDescFirst === undefined ? {} : { sortDescFirst }),
  });
  if (rows.length === 0) return null;

  return (
    <div>
      <PanelSectionTitle>{title}</PanelSectionTitle>
      {description}
      <div className="overflow-x-auto rounded-lg border border-edge">
        <table className="w-full text-left font-mono text-xs text-slate-200">
          <thead className="bg-surface text-slate-400">
            {table.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id}>
                {headerGroup.headers.map((header) => {
                  const canSort = header.column.getCanSort();
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
                        !canSort
                          ? undefined
                          : sortDirection === "asc"
                            ? "ascending"
                            : sortDirection === "desc"
                              ? "descending"
                              : "none"
                      }
                      className="whitespace-nowrap px-3 py-2 font-semibold"
                    >
                      {canSort ? (
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
                      ) : (
                        <table.FlexRender header={header} />
                      )}
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
                  cell.column.id === rowHeaderColumnId ? (
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

export default StatisticsTable;
