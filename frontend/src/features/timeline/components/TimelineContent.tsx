import { filterTimelineRows } from "../lib/filterTimelineRows";
import { isSelectableRow, type TimelineData } from "../lib/timelineRow";
import { useTimelineSearchState } from "../hooks/useTimelineSearchState";
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
  rows.filter((row) => row.kind !== "boundary" && row.kind !== "gap").length;

const TimelineContent = ({ timeline, table }: TimelineContentProps) => {
  const branchOptions = [...branchAccentsByName(timeline.rows).keys()];
  const {
    kindFilter,
    branchFilter: activeBranchFilter,
    requestedEvent,
    setKindFilter,
    setBranchFilter,
    clearFilters: handleClearFilters,
    setEvent,
  } = useTimelineSearchState({
    isKnownBranch: (branchName) => branchOptions.includes(branchName),
    isSelectableEvent: (filePath) =>
      timeline.rows.some(
        (row) => isSelectableRow(row) && row.filePath === filePath,
      ),
  });
  const shownRows = filterTimelineRows(
    timeline.rows,
    kindFilter,
    activeBranchFilter,
  );
  const selectedRow =
    shownRows
      .filter(isSelectableRow)
      .find((row) => row.filePath === requestedEvent) ?? null;
  const selectedFilePath = selectedRow?.filePath ?? null;
  const { handleSelect, clearSelection } = useTimelineSelection(
    shownRows,
    selectedFilePath,
    setEvent,
  );

  const commitCount = countEventRows(timeline.rows);
  const isFiltering = kindFilter !== "all" || activeBranchFilter !== null;

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
