import { useState } from "react";
import { Link } from "@tanstack/react-router";
import { useTable } from "@tanstack/react-table";
import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
import type { TimelineData } from "../../lib/timelineRow";
import { branchAccentsByName } from "../eventColor";
import TimelineEmptyState from "../TimelineEmptyState";
import { HEADER_CLASS_BY_COLUMN } from "./columnClasses";
import { buildTimelineColumns, timelineTableFeatures } from "./timelineColumns";
import TimelineTableRow from "./TimelineTableRow";

interface TimelineListProps {
  timeline: TimelineData;
  table: string | undefined;
  selectedFilePath: string | null;
  onSelect: (filePath: string) => void;
}

const TimelineList = ({
  timeline,
  table,
  selectedFilePath,
  onSelect,
}: TimelineListProps) => {
  const { rows, skippedNodeCount, olderCommitCount } = timeline;
  const [nowMs] = useState(() => Date.now());
  const branchAccents = branchAccentsByName(rows);

  const timelineTable = useTable({
    features: timelineTableFeatures,
    columns: buildTimelineColumns(nowMs, branchAccents),
    data: rows,
    getRowId: (row) => row.filePath,
  });

  const hasEventRows = rows.some((row) => row.kind !== "boundary");
  if (!hasEventRows) {
    return <TimelineEmptyState tableName={table} />;
  }

  const skippedWord = skippedNodeCount === 1 ? "commit" : "commits";
  const skippedLabel = `${skippedNodeCount.toString()} ${skippedWord} could not be read`;
  const commitsWord = olderCommitCount === 1 ? "commit" : "commits";
  const olderHistoryLabel = `↓ ${olderCommitCount.toString()} earlier ${commitsWord} not loaded`;

  return (
    <div className="mx-auto max-w-5xl px-3">
      {skippedNodeCount > 0 && (
        <p className="px-3 pb-4 text-xs text-amber-400">{skippedLabel}</p>
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
      {olderCommitCount > 0 && (
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
