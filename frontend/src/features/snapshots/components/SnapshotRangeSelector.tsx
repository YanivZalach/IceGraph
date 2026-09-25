import type { SnapshotEntry, SnapshotRange } from "../snapshotRange";
import {
  UI_BODY_MUTED_CLASS,
  UI_PAGE_TITLE_CLASS,
} from "../../../uiTypography";
import SnapshotRangePicker from "./SnapshotRangePicker";

interface SnapshotRangeSelectorProps {
  tableName: string;
  entries: readonly SnapshotEntry[];
  range: SnapshotRange;
  onRangeChange: (range: SnapshotRange) => void;
  onGenerate: () => void;
}

const SnapshotRangeSelector = ({
  tableName,
  entries,
  range,
  onRangeChange,
  onGenerate,
}: SnapshotRangeSelectorProps) => (
  <>
    <h2 className={`${UI_PAGE_TITLE_CLASS} mb-2`}>Select Snapshots</h2>
    {entries.length === 0 ? (
      <p className={UI_BODY_MUTED_CLASS}>
        <strong className="break-all text-ink">{tableName}</strong> has no
        snapshots yet. You can still generate the graph.
      </p>
    ) : (
      <>
        <p className={`${UI_BODY_MUTED_CLASS} mb-4`}>
          Choose an inclusive snapshot range for{" "}
          <strong className="break-all text-ink">{tableName}</strong>.{" "}
          {range.anchorSnapshotId === null
            ? "Click a snapshot to start a new range, then click another to finish it."
            : "Click another snapshot, Latest, or Full history to finish the range."}
        </p>
        <SnapshotRangePicker
          entries={entries}
          range={range}
          onRangeChange={onRangeChange}
        />
      </>
    )}
    <button
      type="button"
      onClick={onGenerate}
      className="mt-5 w-full cursor-pointer rounded-lg bg-accent py-3 font-bold text-white transition-colors hover:bg-accent-dark"
    >
      Generate Graph
    </button>
  </>
);
export default SnapshotRangeSelector;
