import {
  formatAbsoluteTimestamp,
  formatEventTime,
} from "../../lib/format/formatTimelineTime";

interface TimeCellProps {
  timestampMs: number;
  nowMs: number;
}

const TimeCell = ({ timestampMs, nowMs }: TimeCellProps) => (
  <time
    dateTime={new Date(timestampMs).toISOString()}
    title={formatAbsoluteTimestamp(timestampMs)}
  >
    {formatEventTime(timestampMs, nowMs)}
  </time>
);

export default TimeCell;
