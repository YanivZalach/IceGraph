import { useState } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import {
  filterTimelineRows,
  type CommitKindFilter,
} from "../lib/filterTimelineRows";
import type { TimelineData } from "../lib/timelineRow";
import { useTimelineSelection } from "../hooks/useTimelineSelection";
import EventDetailsPanel from "./details/EventDetailsPanel";
import TimelineList from "./list/TimelineList";

interface TimelineContentProps {
  timeline: TimelineData;
  table: string | undefined;
}

const TimelineContent = ({ timeline, table }: TimelineContentProps) => {
  const [kindFilter, setKindFilter] = useState<CommitKindFilter>("all");
  const [branchFilter, setBranchFilter] = useState<string | null>(null);
  const shownRows = filterTimelineRows(timeline.rows, kindFilter, branchFilter);
  const { selectedFilePath, handleSelect, clearSelection } =
    useTimelineSelection(shownRows);
  const selectedRow =
    shownRows.find((row) => row.filePath === selectedFilePath) ?? null;

  return (
    <div className="h-graph flex-1 overflow-hidden bg-canvas">
      <Group orientation="horizontal">
        <Panel
          id="timeline-list"
          minSize="40%"
          className="overflow-y-auto py-3 [scrollbar-gutter:stable]"
        >
          <TimelineList
            timeline={timeline}
            rows={shownRows}
            table={table}
            kindFilter={kindFilter}
            onKindFilterChange={setKindFilter}
            branchFilter={branchFilter}
            onBranchFilterChange={setBranchFilter}
            selectedFilePath={selectedFilePath}
            onSelect={handleSelect}
          />
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
