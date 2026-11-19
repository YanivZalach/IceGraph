import { Fragment, useState } from "react";
import { Link } from "@tanstack/react-router";
import { useTable } from "@tanstack/react-table";
import { cn } from "../../../../shared/lib/cn";
import { UI_CAPTION_LABEL_SLATE400_CLASS } from "../../../../uiTypography";
import {
  dayKeyOf,
  formatDayHeading,
} from "../../lib/format/formatTimelineTime";
import type { TimelineData, TimelineRow } from "../../lib/timelineRow";
import { branchAccentsByName } from "../eventColor";
import TimelineEmptyState from "../TimelineEmptyState";
import EventRail from "./EventRail";
import {
  COLUMN_COUNT,
  HEADER_CLASS_BY_COLUMN,
  RAIL_CELL_CLASS,
} from "./columnClasses";
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
  const branchAccents = branchAccentsByName(timeline.rows);

  const timelineTable = useTable({
    features: timelineTableFeatures,
    columns: buildTimelineColumns(nowMs, branchAccents),
    data: rows,
    getRowId: (row) => row.filePath,
  });

  const hasEventRows = timeline.rows.some((row) => row.kind !== "boundary");
  if (!hasEventRows) {
    return (
      <TimelineEmptyState
        tableName={table}
        unreadableCommitCount={timeline.unreadableCommitCount}
      />
    );
  }

  const hasMatches = rows.length > 0;
  if (!hasMatches) {
    return (
      <p className="flex items-baseline gap-1.5 px-3 py-6 text-xl text-slate-400">
        <span>No events match the filters.</span>
        <button
          type="button"
          onClick={onClearFilters}
          className="cursor-pointer text-accent-text hover:underline"
        >
          Clear filters
        </button>
      </p>
    );
  }

  const eventsWord = olderCommitCount === 1 ? "event" : "events";
  const olderHistoryLabel = `↓ ${olderCommitCount.toString()} earlier ${eventsWord} not loaded`;

  return (
    <div>
      <div className="overflow-x-auto rounded-xl border border-edge bg-surface">
        <table className="w-full min-w-[36rem]">
          <thead>
            {timelineTable.getHeaderGroups().map((headerGroup) => (
              <tr key={headerGroup.id} className="border-b border-edge">
                {headerGroup.headers.map((header) => (
                  <th
                    key={header.id}
                    className={cn(
                      UI_CAPTION_LABEL_SLATE400_CLASS,
                      "text-sm",
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
                    <tr>
                      <td className={RAIL_CELL_CLASS}>
                        {previousRow !== undefined && (
                          <EventRail marker="none" />
                        )}
                      </td>
                      <th
                        scope="rowgroup"
                        colSpan={COLUMN_COUNT - 1}
                        className="py-3 text-left text-sm font-normal text-slate-400"
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
      {olderCommitCount > 0 && !isFiltering && (
        <Link
          to="/snapshots-selection"
          search={{ table }}
          className="block px-3 py-3 text-lg text-slate-400 italic hover:text-ink hover:underline"
        >
          {olderHistoryLabel}
        </Link>
      )}
    </div>
  );
};

export default TimelineList;
