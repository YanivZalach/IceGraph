import { useState, type ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../uiTypography";
import {
  dayKeyOf,
  formatAbsoluteTimestamp,
  formatDayHeading,
  formatEventTime,
  parseUtcTimestampToMillis,
} from "../../timeline/lib/format/formatTimelineTime";
import type { SnapshotEntry } from "../snapshotRange";
import SnapshotBadges from "./SnapshotBadges";
import type { SnapshotRowMarking } from "./SnapshotRail";
import SnapshotTableRow from "./SnapshotTableRow";

interface SnapshotTableProps {
  ariaLabel: string;
  entries: readonly SnapshotEntry[];
  markingOf: (entry: SnapshotEntry, index: number) => SnapshotRowMarking;
  onSelect: (snapshotId: string) => void;
  leadingRows?: ReactNode;
  trailingRows?: ReactNode;
}

export const SNAPSHOT_TABLE_COLUMN_COUNT = 4;

const OPERATION_CHIP_CLASSES: Record<string, string> = {
  overwrite: "bg-blue-500/15 text-blue-400",
  append: "bg-emerald-500/15 text-emerald-400",
  replace: "bg-amber-500/15 text-amber-400",
  delete: "bg-rose-500/15 text-rose-400",
};

const HEADER_CLASS = cn(UI_FIELD_LABEL_CLASS, "py-3 pr-6");

const timestampOf = (entry: SnapshotEntry | undefined): number | null =>
  entry === undefined ? null : parseUtcTimestampToMillis(entry.timestamp);

interface DayGroup {
  dayKey: string;
  heading: string;
  rows: { entry: SnapshotEntry; index: number }[];
}

const groupByDay = (
  entries: readonly SnapshotEntry[],
  nowMs: number,
): DayGroup[] => {
  const groups: DayGroup[] = [];
  entries.forEach((entry, index) => {
    const timestampMs = timestampOf(entry);
    const dayKey = timestampMs === null ? "" : dayKeyOf(timestampMs);
    const lastGroup = groups.at(-1);
    if (lastGroup?.dayKey === dayKey) {
      lastGroup.rows.push({ entry, index });
      return;
    }
    groups.push({
      dayKey,
      heading:
        timestampMs === null
          ? "Unknown time"
          : formatDayHeading(timestampMs, nowMs),
      rows: [{ entry, index }],
    });
  });

  return groups;
};

const SnapshotTable = ({
  ariaLabel,
  entries,
  markingOf,
  onSelect,
  leadingRows,
  trailingRows,
}: SnapshotTableProps) => {
  const [nowMs] = useState(() => Date.now());

  return (
    <div className="max-h-80 overflow-auto rounded-xl border border-edge bg-surface [scrollbar-gutter:stable]">
      <table aria-label={ariaLabel} className="w-full min-w-[30rem]">
        <thead className="sticky top-0 z-10 bg-surface-deep">
          <tr>
            <th className="w-10 min-w-10">
              <span className="sr-only">Range</span>
            </th>
            <th className={cn(HEADER_CLASS, "text-left")}>Operation</th>
            <th className={cn(HEADER_CLASS, "text-right")}>Snapshot</th>
            <th className={cn(HEADER_CLASS, "pr-5 text-right")}>Time</th>
          </tr>
        </thead>
        {leadingRows !== undefined && <tbody>{leadingRows}</tbody>}
        {groupByDay(entries, nowMs).map((group, groupIndex) => (
          <tbody key={`${group.dayKey}-${groupIndex.toString()}`}>
            <tr className="border-t border-edge bg-canvas/50">
              <th
                scope="rowgroup"
                colSpan={SNAPSHOT_TABLE_COLUMN_COUNT}
                className="px-4 py-2 text-left text-xs font-semibold tracking-wide text-slate-400"
              >
                {group.heading}
              </th>
            </tr>
            {group.rows.map(({ entry, index }) => {
              const timestampMs = timestampOf(entry);
              const marking = markingOf(entry, index);
              return (
                <SnapshotTableRow
                  key={entry.snapshotId}
                  label={[`Snapshot ${entry.snapshotId}`, entry.operation]
                    .filter(Boolean)
                    .join(", ")}
                  marking={marking}
                  onSelect={() => {
                    onSelect(entry.snapshotId);
                  }}
                >
                  <td className="py-3 pr-6">
                    <span className="flex items-center gap-2">
                      {entry.operation !== "" && (
                        <span
                          className={cn(
                            "rounded-md px-2 py-px font-mono text-xs whitespace-nowrap",
                            OPERATION_CHIP_CLASSES[entry.operation] ??
                              "bg-slate-500/15 text-slate-400",
                          )}
                        >
                          {entry.operation}
                        </span>
                      )}
                      <SnapshotBadges badges={marking.badges} />
                    </span>
                  </td>
                  <td
                    className={cn(
                      "py-3 pr-6 text-right font-mono text-xs whitespace-nowrap",
                      marking.isEdge ? "text-slate-300" : "text-slate-500",
                    )}
                  >
                    {entry.snapshotId}
                  </td>
                  <td className="py-3 pr-5 text-right text-xs whitespace-nowrap text-slate-400">
                    {timestampMs !== null && (
                      <time
                        dateTime={new Date(timestampMs).toISOString()}
                        title={formatAbsoluteTimestamp(timestampMs)}
                      >
                        {formatEventTime(timestampMs, nowMs)}
                      </time>
                    )}
                  </td>
                </SnapshotTableRow>
              );
            })}
          </tbody>
        ))}
        {trailingRows !== undefined && <tbody>{trailingRows}</tbody>}
      </table>
    </div>
  );
};

export default SnapshotTable;
