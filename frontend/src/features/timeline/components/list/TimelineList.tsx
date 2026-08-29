import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { useTable } from "@tanstack/react-table";
import { cn } from "../../../../shared/lib/cn";
import {
  UI_FIELD_LABEL_CLASS,
  UI_TEXT_INPUT_CLASS,
} from "../../../../uiTypography";
import {
  commitKindFilterSchema,
  type CommitKindFilter,
} from "../../lib/filterTimelineRows";
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
  kindFilter: CommitKindFilter;
  onKindFilterChange: (kind: CommitKindFilter) => void;
  branchFilter: string | null;
  onBranchFilterChange: (branchName: string | null) => void;
  selectedFilePath: string | null;
  onSelect: (filePath: string) => void;
}

const TimelineList = ({
  timeline,
  rows,
  table,
  kindFilter,
  onKindFilterChange,
  branchFilter,
  onBranchFilterChange,
  selectedFilePath,
  onSelect,
}: TimelineListProps) => {
  const { unreadableCommitCount, olderCommitCount } = timeline;
  const [nowMs] = useState(() => Date.now());
  const branchAccents = branchAccentsByName(timeline.rows);

  const timelineTable = useTable({
    features: timelineTableFeatures,
    columns: buildTimelineColumns(nowMs, branchAccents),
    data: rows,
    getRowId: (row) => row.filePath,
  });

  const unreadableWord = unreadableCommitCount === 1 ? "commit" : "commits";
  const unreadableBanner = unreadableCommitCount > 0 && (
    <p className="px-3 pb-4 text-xs text-amber-400">
      {`${unreadableCommitCount.toString()} ${unreadableWord} could not be read`}
    </p>
  );

  const hasEventRows = timeline.rows.some((row) => row.kind !== "boundary");
  if (!hasEventRows) {
    return (
      <div className="mx-auto max-w-5xl px-3">
        {unreadableBanner}
        <TimelineEmptyState tableName={table} />
      </div>
    );
  }

  const commitsWord = olderCommitCount === 1 ? "commit" : "commits";
  const olderHistoryLabel = `↓ ${olderCommitCount.toString()} earlier ${commitsWord} not loaded`;
  const isFiltering = kindFilter !== "all" || branchFilter !== null;
  const hasMatches = rows.length > 0;
  const branchOptions = [...branchAccents.keys()];

  return (
    <div className="mx-auto max-w-5xl px-3">
      {unreadableBanner}
      <div className="mb-2 flex justify-end gap-2">
        {branchOptions.length > 0 && (
          <select
            value={branchFilter ?? ""}
            onChange={(event) => {
              onBranchFilterChange(
                event.target.value === "" ? null : event.target.value,
              );
            }}
            aria-label="Filter by branch"
            className={cn(UI_TEXT_INPUT_CLASS, "w-auto py-1 text-xs")}
          >
            <option value="">All branches</option>
            {branchOptions.map((branchName) => (
              <option key={branchName} value={branchName}>
                {branchName}
              </option>
            ))}
          </select>
        )}
        <select
          value={kindFilter}
          onChange={(event) => {
            onKindFilterChange(
              commitKindFilterSchema.parse(event.target.value),
            );
          }}
          aria-label="Filter by commit kind"
          className={cn(UI_TEXT_INPUT_CLASS, "w-auto py-1 text-xs")}
        >
          <option value="all">All commits</option>
          <option value="writes">Data writes</option>
          <option value="metadata">Metadata changes</option>
        </select>
      </div>
      {!hasMatches && (
        <p className="px-3 py-6 text-sm text-slate-500">
          No commits match the filters.
        </p>
      )}
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
