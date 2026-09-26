import {
  isEntryInRange,
  selectFullHistoryStart,
  selectLatestEnd,
  selectSnapshot,
  type SnapshotEntry,
  type SnapshotRange,
} from "../snapshotRange";
import { formatLocaleDateTime, parseUtcDate } from "../../../utils/dateUtils";
import SnapshotRangeRow from "./SnapshotRangeRow";

interface SnapshotRangePickerProps {
  entries: readonly SnapshotEntry[];
  range: SnapshotRange;
  onRangeChange: (range: SnapshotRange) => void;
}

const SnapshotTime = ({ timestamp }: { timestamp: string }) => {
  const date = parseUtcDate(timestamp);
  const text = date ? formatLocaleDateTime(date) : timestamp;
  const precisionIndex = text.indexOf(".");
  if (precisionIndex === -1) return text;
  return (
    <>
      {text.slice(0, precisionIndex)}
      <span className="font-normal text-slate-500">
        {text.slice(precisionIndex)}
      </span>
    </>
  );
};

const SnapshotRangePicker = ({
  entries,
  range,
  onRangeChange,
}: SnapshotRangePickerProps) => (
  <ol
    aria-label="Snapshots, newest first"
    className="max-h-72 overflow-y-auto rounded-xl bg-canvas px-3 py-2"
  >
    <SnapshotRangeRow
      label="Latest"
      detail="Always ends at the newest snapshot when the graph loads"
      isStart={false}
      isEnd={range.endSnapshotId === ""}
      isInRange={range.endSnapshotId === ""}
      isAnchor={false}
      onSelect={() => {
        onRangeChange(selectLatestEnd(range));
      }}
    />
    {entries.map((entry, index) => (
      <SnapshotRangeRow
        key={entry.snapshotId}
        label={<SnapshotTime timestamp={entry.timestamp} />}
        detail={`ID: ${entry.snapshotId}`}
        operation={entry.operation}
        isStart={range.startSnapshotId === entry.snapshotId}
        isEnd={range.endSnapshotId === entry.snapshotId}
        isInRange={isEntryInRange(range, entries, index)}
        isAnchor={range.anchorSnapshotId === entry.snapshotId}
        onSelect={() => {
          onRangeChange(selectSnapshot(range, entries, entry.snapshotId));
        }}
      />
    ))}
    <SnapshotRangeRow
      label="Full history"
      detail="Starts from the table's first snapshot. Not recommended: slow and cluttered on large tables"
      isStart={range.startSnapshotId === ""}
      isEnd={false}
      isInRange={range.startSnapshotId === ""}
      isAnchor={false}
      onSelect={() => {
        onRangeChange(selectFullHistoryStart(range));
      }}
    />
  </ol>
);
export default SnapshotRangePicker;
