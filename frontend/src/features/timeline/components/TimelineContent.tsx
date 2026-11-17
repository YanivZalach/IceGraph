import { useState } from "react";
import {
  filterTimelineRows,
  type CommitKindFilter,
} from "../lib/filterTimelineRows";
import type { TimelineData } from "../lib/timelineRow";
import { useTimelineSelection } from "../hooks/useTimelineSelection";
import EventDetailsPanel from "./details/EventDetailsPanel";
import { branchAccentsByName } from "./eventColor";
import TimelineList from "./list/TimelineList";
import TimelineToolbar from "./list/TimelineToolbar";

interface TimelineContentProps {
  timeline: TimelineData;
  table: string | undefined;
}

const countEventRows = (rows: TimelineData["rows"]): number =>
  rows.filter((row) => row.kind !== "boundary").length;

const TimelineContent = ({ timeline, table }: TimelineContentProps) => {
  const [kindFilter, setKindFilter] = useState<CommitKindFilter>("all");
  const [branchFilter, setBranchFilter] = useState<string | null>(null);
  const branchOptions = [...branchAccentsByName(timeline.rows).keys()];
  // A new snapshot range can drop the filtered branch; the filter is cleared
  // rather than hidden, so it cannot come back when a later range has it.
  if (branchFilter !== null && !branchOptions.includes(branchFilter)) {
    setBranchFilter(null);
  }
  const activeBranchFilter =
    branchFilter !== null && branchOptions.includes(branchFilter)
      ? branchFilter
      : null;
  const shownRows = filterTimelineRows(
    timeline.rows,
    kindFilter,
    activeBranchFilter,
  );
  const { selectedFilePath, handleSelect, clearSelection } =
    useTimelineSelection(shownRows);
  const selectedRow =
    shownRows.find((row) => row.filePath === selectedFilePath) ?? null;

  const commitCount = countEventRows(timeline.rows);
  const isFiltering = kindFilter !== "all" || activeBranchFilter !== null;

  const handleClearFilters = () => {
    setKindFilter("all");
    setBranchFilter(null);
  };

  return (
    <div className="h-graph flex w-full min-h-0 flex-1 flex-col overflow-hidden bg-canvas md:flex-row">
      <div className="min-h-0 min-w-0 flex-1 overflow-y-auto [scrollbar-gutter:stable]">
        {commitCount > 0 && (
          <TimelineToolbar
            commitCount={commitCount}
            shownCommitCount={countEventRows(shownRows)}
            unreadableCommitCount={timeline.unreadableCommitCount}
            isTableMetadataUnreadable={timeline.isTableMetadataUnreadable}
            kindFilter={kindFilter}
            onKindFilterChange={setKindFilter}
            branchOptions={branchOptions}
            branchFilter={activeBranchFilter}
            onBranchFilterChange={setBranchFilter}
          />
        )}
        <div className="mx-auto max-w-6xl px-3 py-4 sm:px-5">
          <TimelineList
            timeline={timeline}
            rows={shownRows}
            table={table}
            isFiltering={isFiltering}
            onClearFilters={handleClearFilters}
            selectedFilePath={selectedFilePath}
            onSelect={handleSelect}
          />
        </div>
      </div>

      {selectedRow !== null && (
        <EventDetailsPanel
          timeline={timeline}
          row={selectedRow}
          onClose={clearSelection}
        />
      )}
    </div>
  );
};

export default TimelineContent;
