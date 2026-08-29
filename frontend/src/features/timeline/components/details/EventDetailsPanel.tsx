import { PanelHeader } from "../../../../components/PanelContent";
import { formatAbsoluteTimestamp } from "../../lib/format/formatTimelineTime";
import type { TimelineData, TimelineRow } from "../../lib/timelineRow";
import { cn } from "../../../../shared/lib/cn";
import { eventBorderClassFor, eventColorFor } from "../eventColor";
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
}: EventDetailsPanelProps) => (
  <aside
    className={cn(
      "flex h-full flex-col border-l-4 bg-surface",
      eventBorderClassFor(row),
    )}
  >
    <div className="flex shrink-0 items-start justify-between gap-2 border-b border-edge px-5 py-4">
      <PanelHeader
        title={row.title}
        titleColor={eventColorFor(row)}
        subtitle={row.filePath}
        meta={formatAbsoluteTimestamp(row.timestampMs)}
      />
      <button
        type="button"
        aria-label="Close details"
        onClick={onClose}
        className="flex size-7 shrink-0 cursor-pointer items-center justify-center rounded-full bg-edge text-slate-400 transition hover:bg-edge-hover hover:text-slate-200 focus-visible:ring-1 focus-visible:ring-slate-300/70 focus-visible:outline-none"
      >
        ✕
      </button>
    </div>
    <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4">
      <EventDetails timeline={timeline} row={row} />
    </div>
  </aside>
);

export default EventDetailsPanel;
