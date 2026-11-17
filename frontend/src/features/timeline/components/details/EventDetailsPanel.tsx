import type { CSSProperties } from "react";
import { PanelHeader } from "../../../../components/PanelContent";
import SidePanelFrame, {
  SidePanelResizeHandle,
} from "../../../../components/SidePanelFrame";
import { useDockedPanelWidth } from "../../../../components/useDockedPanelWidth";
import { formatAbsoluteTimestamp } from "../../lib/format/formatTimelineTime";
import type { TimelineData, TimelineRow } from "../../lib/timelineRow";
import { eventToneFor } from "../eventColor";
import EventDetails from "./EventDetails";

interface EventDetailsPanelProps {
  timeline: TimelineData;
  row: TimelineRow;
  onClose: () => void;
}

interface EventDetailsPanelStyle extends CSSProperties {
  "--timeline-details-width": string;
  "--panel-accent": string;
}

const PANEL_MIN_WIDTH_PX = 320;
const PANEL_MAX_CONTAINER_SHARE = 0.7;

const EventDetailsPanel = ({
  timeline,
  row,
  onClose,
}: EventDetailsPanelProps) => {
  const { panelWidthPx, startResize } = useDockedPanelWidth(
    PANEL_MIN_WIDTH_PX,
    PANEL_MAX_CONTAINER_SHARE,
  );
  const accentColor = eventToneFor(row).color;
  const panelStyle: EventDetailsPanelStyle = {
    "--timeline-details-width":
      panelWidthPx === null ? "35%" : `${String(panelWidthPx)}px`,
    "--panel-accent": accentColor,
  };

  return (
    <SidePanelFrame
      variant="docked"
      ariaLabel="Commit details"
      closeLabel="Close details"
      className="h-[55%] min-h-0 w-full shrink-0 border-t border-edge md:h-auto md:w-[var(--timeline-details-width)] md:min-w-[20rem] md:max-w-[70%] md:border-t-0"
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
      leading={
        <SidePanelResizeHandle
          accentColor={accentColor}
          className="hidden md:block"
          isEdgeOnly
          onMouseDown={startResize}
          title="Drag left or right to resize"
        />
      }
      onClose={onClose}
      style={panelStyle}
    >
      <EventDetails timeline={timeline} row={row} />
    </SidePanelFrame>
  );
};

export default EventDetailsPanel;
