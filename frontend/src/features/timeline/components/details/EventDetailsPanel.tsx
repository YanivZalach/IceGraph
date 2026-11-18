import { PanelHeader } from "../../../../components/PanelContent";
import SidePanelFrame from "../../../../components/SidePanelFrame";
import { formatAbsoluteTimestamp } from "../../lib/format/formatTimelineTime";
import type { TimelineData, TimelineRow } from "../../lib/timelineRow";
import { eventToneFor } from "../eventColor";
import EventDetails from "./EventDetails";

interface EventDetailsPanelProps {
  timeline: TimelineData;
  row: TimelineRow;
  onClose: () => void;
}

const EventDetailsPanel = ({
  timeline,
  row,
  onClose,
}: EventDetailsPanelProps) => {
  const accentColor = eventToneFor(row).color;
  return (
    <SidePanelFrame
      variant="docked"
      ariaLabel="Commit details"
      closeLabel="Close details"
      className="h-full min-h-0 w-full"
      contentClassName="gap-4 overscroll-contain"
      header={
        <PanelHeader
          title={row.title}
          titleColor={accentColor}
          subtitle={row.filePath}
          meta={formatAbsoluteTimestamp(row.timestampMs)}
          preserveSubtitleEnd
        />
      }
      onClose={onClose}
    >
      <EventDetails timeline={timeline} row={row} />
    </SidePanelFrame>
  );
};

export default EventDetailsPanel;
