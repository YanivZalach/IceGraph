import {
  isEntryInRange,
  selectFullHistoryStart,
  selectLatestEnd,
  selectSnapshot,
  type SnapshotEntry,
  type SnapshotRange,
} from "../snapshotRange";
import SnapshotBoundaryRow from "./SnapshotBoundaryRow";
import SnapshotRail, { type SnapshotRowMarking } from "./SnapshotRail";
import SnapshotTable, { SNAPSHOT_TABLE_COLUMN_COUNT } from "./SnapshotTable";

interface SnapshotRangePickerProps {
  entries: readonly SnapshotEntry[];
  range: SnapshotRange;
  onRangeChange: (range: SnapshotRange) => void;
  hasOlderSnapshots: boolean;
  isLoadingOlderSnapshots: boolean;
  olderSnapshotsError: string | null;
  onLoadOlderSnapshots: () => void;
}

const CONTENT_COLUMN_SPAN = SNAPSHOT_TABLE_COLUMN_COUNT - 1;

const rangeMarking = (
  isStart: boolean,
  isEnd: boolean,
  isInRange: boolean,
  isAnchor: boolean,
): SnapshotRowMarking => ({
  isEdge: isStart || isEnd,
  isInRange,
  isAnchor,
  badges: [...(isStart ? ["Start"] : []), ...(isEnd ? ["End"] : [])],
});

const SnapshotRangePicker = ({
  entries,
  range,
  onRangeChange,
  hasOlderSnapshots,
  isLoadingOlderSnapshots,
  olderSnapshotsError,
  onLoadOlderSnapshots,
}: SnapshotRangePickerProps) => {
  const isFullHistory = range.startSnapshotId === "";

  return (
    <SnapshotTable
      ariaLabel="Snapshots, newest first"
      entries={entries}
      markingOf={(entry, index) =>
        rangeMarking(
          range.startSnapshotId === entry.snapshotId,
          range.endSnapshotId === entry.snapshotId,
          isEntryInRange(range, entries, index),
          range.anchorSnapshotId === entry.snapshotId,
        )
      }
      onSelect={(snapshotId) => {
        onRangeChange(selectSnapshot(range, entries, snapshotId));
      }}
      leadingRows={
        <SnapshotBoundaryRow
          label="Latest"
          detail="Always ends at the newest snapshot when the graph loads"
          marking={rangeMarking(
            false,
            range.endSnapshotId === "",
            range.endSnapshotId === "",
            false,
          )}
          onSelect={() => {
            onRangeChange(selectLatestEnd(range));
          }}
        />
      }
      trailingRows={
        <>
          {hasOlderSnapshots && (
            <tr className="border-t border-edge">
              <td className="relative w-10 min-w-10">
                <SnapshotRail
                  marking={rangeMarking(false, false, isFullHistory, false)}
                  hasNode={false}
                />
              </td>
              <td colSpan={CONTENT_COLUMN_SPAN} className="py-2 pr-5">
                <button
                  type="button"
                  disabled={isLoadingOlderSnapshots}
                  onClick={onLoadOlderSnapshots}
                  className="w-full cursor-pointer rounded-lg border border-dashed border-edge px-3 py-2 text-sm font-semibold text-slate-300 transition hover:border-edge-hover hover:bg-surface-hover disabled:cursor-wait disabled:opacity-60"
                >
                  {isLoadingOlderSnapshots
                    ? "Loading older snapshots..."
                    : "Load older snapshots"}
                </button>
                {olderSnapshotsError !== null && (
                  <p role="alert" className="mt-1 text-xs text-red-400">
                    Failed to load older snapshots: {olderSnapshotsError}
                  </p>
                )}
              </td>
            </tr>
          )}
          <SnapshotBoundaryRow
            label="Full history"
            detail="Starts from the table's first snapshot. Not recommended: slow and cluttered on large tables"
            marking={rangeMarking(isFullHistory, false, isFullHistory, false)}
            onSelect={() => {
              onRangeChange(selectFullHistoryStart(range));
            }}
          />
        </>
      }
    />
  );
};

export default SnapshotRangePicker;
