import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { useTable } from "@tanstack/react-table";
import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
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
    return <TimelineEmptyState tableName={table} />;
  }

  const hasMatches = rows.length > 0;
  if (!hasMatches) {
    return (
      <p className="flex items-baseline gap-1.5 px-6 py-6 text-sm text-slate-500">
        <span>No commits match the filters.</span>
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

  const commitsWord = olderCommitCount === 1 ? "commit" : "commits";
  const olderHistoryLabel = `↓ ${olderCommitCount.toString()} earlier ${commitsWord} not loaded`;

  return (
    <div className="mx-auto max-w-5xl px-3">
      <table className="w-full">
        <thead>
          {timelineTable.getHeaderGroups().map((headerGroup) => (
            <tr key={headerGroup.id} className="border-b border-edge/50">
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
          {timelineTable.getRowModel().rows.map((row) => (
            <TimelineTableRow
              key={row.id}
              row={row}
              isSelected={selectedFilePath === row.original.filePath}
              onSelect={onSelect}
            />
          ))}
        </tbody>
      </table>
      {olderCommitCount > 0 && !isFiltering && (
        <Link
          to="/snapshots-selection"
          search={{ table }}
          className="block px-3 py-2 text-xs text-slate-500 italic hover:text-slate-300 hover:underline"
        >
          {olderHistoryLabel}
        </Link>
      )}
    </div>
  );
};

export default TimelineList;
