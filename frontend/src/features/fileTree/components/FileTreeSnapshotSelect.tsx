import { formatSnapshotVersion } from "../model";
import type { SnapshotNode } from "../types";

interface FileTreeSnapshotSelectProps {
  className: string;
  currentSnapshotId: string;
  onChange: (snapshotId: string) => void;
  snapshots: SnapshotNode[];
}

const FileTreeSnapshotSelect = ({
  className,
  currentSnapshotId,
  onChange,
  snapshots,
}: FileTreeSnapshotSelectProps) => (
  <label className="flex flex-col gap-1.5 text-xs font-bold uppercase tracking-wide text-slate-500">
    Snapshot
    <select
      value={currentSnapshotId}
      disabled={snapshots.length === 0}
      onChange={(event) => {
        onChange(event.target.value);
      }}
      className={className}
    >
      {snapshots.length === 0 ? (
        <option value="">No snapshots in loaded range</option>
      ) : null}
      {snapshots.map((snapshot, index) => {
        const snapshotId = snapshot.details.snapshot_id ?? snapshot.id;
        return (
          <option key={snapshot.id} value={snapshotId}>
            {formatSnapshotVersion(snapshot, index === snapshots.length - 1)}
          </option>
        );
      })}
    </select>
  </label>
);

export default FileTreeSnapshotSelect;
