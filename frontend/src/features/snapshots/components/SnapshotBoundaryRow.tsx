import SnapshotBadges from "./SnapshotBadges";
import type { SnapshotRowMarking } from "./SnapshotRail";
import { SNAPSHOT_TABLE_COLUMN_COUNT } from "./SnapshotTable";
import SnapshotTableRow from "./SnapshotTableRow";

interface SnapshotBoundaryRowProps {
  label: string;
  detail: string;
  marking: SnapshotRowMarking;
  onSelect: () => void;
}

const SnapshotBoundaryRow = ({
  label,
  detail,
  marking,
  onSelect,
}: SnapshotBoundaryRowProps) => (
  <SnapshotTableRow label={label} marking={marking} onSelect={onSelect}>
    <td colSpan={SNAPSHOT_TABLE_COLUMN_COUNT - 1} className="max-w-0 py-3 pr-5">
      <span className="flex items-center gap-2">
        <span className="text-sm font-medium text-slate-200">{label}</span>
        <SnapshotBadges badges={marking.badges} />
      </span>
      <span className="mt-0.5 block truncate text-xs text-slate-500">
        {detail}
      </span>
    </td>
  </SnapshotTableRow>
);

export default SnapshotBoundaryRow;
