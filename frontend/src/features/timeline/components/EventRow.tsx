import { cn } from "../../../shared/lib/cn";
import {
  formatAbsoluteTimestamp,
  formatEventTime,
} from "../lib/format/formatTimelineTime";
import type { TimelineRow } from "../lib/timelineRow";
import EventRail from "./EventRail";
import ImpactLine from "./ImpactLine";
import RefBadge from "./RefBadge";
import {
  IMPACT_COLUMN_CLASS,
  ROW_CLASS,
  SHORT_ID_COLUMN_CLASS,
  TIME_COLUMN_CLASS,
  TITLE_COLUMN_CLASS,
} from "./rowColumns";

interface EventRowProps {
  row: TimelineRow;
  isSelected: boolean;
  isCurrent: boolean;
  nowMs: number;
  onSelect: (filePath: string) => void;
}

const EventRow = ({
  row,
  isSelected,
  isCurrent,
  nowMs,
  onSelect,
}: EventRowProps) => (
  <button
    type="button"
    aria-pressed={isSelected}
    onClick={() => {
      onSelect(row.filePath);
    }}
    className={cn(
      ROW_CLASS,
      "transition-colors hover:bg-surface/60",
      isSelected && "bg-surface",
    )}
  >
    <EventRail node={isCurrent ? "current" : "plain"} />

    <span className={TITLE_COLUMN_CLASS}>
      <span
        className={cn("truncate text-sm text-ink", isCurrent && "font-bold")}
      >
        {row.title}
      </span>
      {row.badges.map((badge) => (
        <RefBadge key={badge.name} name={badge.name} type={badge.type} />
      ))}
    </span>

    <span className={cn(IMPACT_COLUMN_CLASS, "text-slate-400")}>
      <ImpactLine segments={row.impact} />
    </span>

    <span className={cn(SHORT_ID_COLUMN_CLASS, "text-slate-500")}>
      {row.shortId}
    </span>

    <time
      className={cn(TIME_COLUMN_CLASS, "text-slate-500")}
      dateTime={new Date(row.timestampMs).toISOString()}
      title={formatAbsoluteTimestamp(row.timestampMs)}
    >
      {formatEventTime(row.timestampMs, nowMs)}
    </time>
  </button>
);

export default EventRow;
