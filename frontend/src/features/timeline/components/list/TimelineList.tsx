import { Fragment, useState } from "react";
import { Link } from "@tanstack/react-router";
import { useTable } from "@tanstack/react-table";
import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
import {
  dayKeyOf,
  formatDayHeading,
} from "../../lib/format/formatTimelineTime";
import type { TimelineData, TimelineRow } from "../../lib/timelineRow";
import { branchAccentsByName } from "../eventColor";
import TimelineEmptyState from "../TimelineEmptyState";
import { HEADER_CLASS_BY_COLUMN } from "./columnClasses";
import { buildTimelineColumns, timelineTableFeatures } from "./timelineColumns";
import TimelineTableRow from "./TimelineTableRow";

interface TimelineListProps {
  timeline: TimelineData;
  rows: TimelineRow[];
  table: string | undefined;
  isFiltering: boolean;
  onClearFilters: () => void;
  selectedFilePath: string | null;
  onSelect: (filePath: string) => void;
}

const COLUMN_COUNT = 5;

const TimelineList = ({
  timeline,
  rows,
  table,
  isFiltering,
  onClearFilters,
  selectedFilePath,
  onSelect,
}: TimelineListProps) => {
  const { olderCommitCount } = timeline;
  const [nowMs] = useState(() => Date.now());
  const branchAccents = branchAccentsByName(
    timeline.rows,
    timeline.branchGraph.branchNames,
  );

  const timelineTable = useTable({
    features: timelineTableFeatures,
    columns: buildTimelineColumns(
      nowMs,
      branchAccents,
      timeline.branchGraph,
      isFiltering,
    ),
    data: rows,
    getRowId: (row) => row.filePath,
  });

  const hasReadableEvents = timeline.rows.some(
    (row) => row.kind !== "boundary" && row.kind !== "gap",
  );
  const emptyState = !hasReadableEvents && (
    <TimelineEmptyState
      tableName={table}
      unreadableCommitCount={timeline.unreadableCommitCount}
    />
  );
  const hasGapRows = timeline.rows.some((row) => row.kind === "gap");
  if (!hasReadableEvents && !hasGapRows) {
    return emptyState;
  }

  const hasMatches = rows.length > 0;
  if (!hasMatches) {
    return (
      <p className="flex items-baseline justify-center gap-1.5 rounded-xl border border-dashed border-edge px-6 py-10 text-sm text-slate-400">
        <span>No events match the filters.</span>
        <button
          type="button"
          onClick={onClearFilters}
          className="cursor-pointer text-accent hover:text-accent-dark hover:underline"
        >
          Clear filters
        </button>
      </p>
    );
  }

  const eventsWord = olderCommitCount === 1 ? "event" : "events";
  const olderHistoryLabel = `↓ ${olderCommitCount.toString()} earlier ${eventsWord} not loaded`;

  return (
    <div className="flex flex-col gap-3">
      {emptyState}
      <div className="overflow-hidden rounded-xl border border-edge bg-surface">
        <div className="overflow-x-auto">
          <table className="w-full min-w-[36rem]">
            <thead className="bg-surface-deep">
              {timelineTable.getHeaderGroups().map((headerGroup) => (
                <tr key={headerGroup.id}>
                  {headerGroup.headers.map((header) => (
                    <th
                      key={header.id}
                      className={cn(
                        UI_FIELD_LABEL_CLASS,
                        HEADER_CLASS_BY_COLUMN[header.column.id],
                      )}
                    >
                      <timelineTable.FlexRender header={header} />
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {timelineTable.getRowModel().rows.map((row, index, tableRows) => {
                const dayKey = dayKeyOf(row.original.timestampMs);
                const previousRow = tableRows[index - 1];
                const isNewDay =
                  previousRow === undefined ||
                  dayKeyOf(previousRow.original.timestampMs) !== dayKey;
                return (
                  <Fragment key={row.id}>
                    {isNewDay && (
                      <tr className="border-t border-edge bg-canvas/50">
                        <th
                          scope="rowgroup"
                          colSpan={COLUMN_COUNT}
                          className="px-4 py-2 text-left text-xs font-semibold tracking-wide text-slate-400"
                        >
                          {formatDayHeading(row.original.timestampMs, nowMs)}
                        </th>
                      </tr>
                    )}
                    <TimelineTableRow
                      row={row}
                      isSelected={selectedFilePath === row.original.filePath}
                      onSelect={onSelect}
                    />
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
      {olderCommitCount > 0 && !isFiltering && (
        <Link
          to="/snapshots-selection"
          search={{ table }}
          className="self-center rounded-lg px-3 py-2 text-xs text-slate-500 italic transition-colors hover:bg-surface hover:text-slate-300"
        >
          {olderHistoryLabel}
        </Link>
      )}
    </div>
  );
};

export default TimelineList;
