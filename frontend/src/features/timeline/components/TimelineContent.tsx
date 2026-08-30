import { useState } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
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
  const shownRows = filterTimelineRows(timeline.rows, kindFilter, branchFilter);
  const { selectedFilePath, handleSelect, clearSelection } =
    useTimelineSelection(shownRows);
  const selectedRow =
    shownRows.find((row) => row.filePath === selectedFilePath) ?? null;

  const commitCount = countEventRows(timeline.rows);
  const isFiltering = kindFilter !== "all" || branchFilter !== null;

  const handleClearFilters = () => {
    setKindFilter("all");
    setBranchFilter(null);
  };

  return (
    <div className="h-graph flex-1 overflow-hidden bg-canvas">
      <Group orientation="horizontal">
        <Panel id="timeline-list" minSize="40%" className="flex flex-col">
          {commitCount > 0 && (
            <TimelineToolbar
              commitCount={commitCount}
              shownCommitCount={countEventRows(shownRows)}
              unreadableCommitCount={timeline.unreadableCommitCount}
              kindFilter={kindFilter}
              onKindFilterChange={setKindFilter}
              branchOptions={[...branchAccentsByName(timeline.rows).keys()]}
              branchFilter={branchFilter}
              onBranchFilterChange={setBranchFilter}
            />
          )}
          <div className="min-h-0 flex-1 overflow-y-auto py-3 [scrollbar-gutter:stable]">
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
        </Panel>

        {selectedRow !== null && (
          <>
            <Separator className="w-1 shrink-0 cursor-ew-resize bg-edge transition-colors hover:bg-accent" />
            <Panel
              id="timeline-details"
              defaultSize="40%"
              minSize="20%"
              maxSize="80%"
            >
              <EventDetailsPanel
                timeline={timeline}
                row={selectedRow}
                onClose={clearSelection}
              />
            </Panel>
          </>
        )}
      </Group>
    </div>
  );
};

export default TimelineContent;
