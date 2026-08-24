import { cn } from "../../../shared/lib/cn";
import {
  formatAbsoluteTimestamp,
  formatEventTime,
} from "../lib/format/formatTimelineTime";
import EventRail from "./EventRail";
import {
  ROW_CLASS,
  SHORT_ID_COLUMN_CLASS,
  TIME_COLUMN_CLASS,
} from "./rowColumns";

interface BoundaryRowProps {
  timestampMs: number;
  nowMs: number;
}

const BoundaryRow = ({ timestampMs, nowMs }: BoundaryRowProps) => (
  <div className={cn(ROW_CLASS, "text-slate-500")}>
    <EventRail node="none" />

    <span className="min-w-0 flex-1 truncate text-xs italic">
      start of loaded range
    </span>

    <span className={SHORT_ID_COLUMN_CLASS} />

    <time
      className={TIME_COLUMN_CLASS}
      dateTime={new Date(timestampMs).toISOString()}
      title={formatAbsoluteTimestamp(timestampMs)}
    >
      {formatEventTime(timestampMs, nowMs)}
    </time>
  </div>
);

export default BoundaryRow;
