import { Group, Panel, Separator } from "react-resizable-panels";
import type { TimelineData } from "../lib/timelineRow";
import { useTimelineSelection } from "../hooks/useTimelineSelection";
import EventDetailsPanel from "./details/EventDetailsPanel";
import TimelineList from "./list/TimelineList";

interface TimelineContentProps {
  timeline: TimelineData;
  table: string | undefined;
}

const TimelineContent = ({ timeline, table }: TimelineContentProps) => {
  const { selectedFilePath, handleSelect, clearSelection } =
    useTimelineSelection(timeline.rows);
  const selectedRow =
    timeline.rows.find((row) => row.filePath === selectedFilePath) ?? null;

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
            table={table}
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
