import { useSearch } from "@tanstack/react-router";

interface NoBranchSnapshotsNoticeProps {
  branchName: string | null;
}

const describeRangeEdge = (snapshotId: string | undefined, fallback: string) =>
  snapshotId === undefined || snapshotId === ""
    ? fallback
    : `snapshot ${snapshotId}`;

const NoBranchSnapshotsNotice = ({
  branchName,
}: NoBranchSnapshotsNoticeProps) => {
  const searchParameters = useSearch({ from: "/table/filetree" });
  const rangeStart = describeRangeEdge(
    searchParameters.start_snapshot_id,
    "the first snapshot",
  );
  const rangeEnd = describeRangeEdge(
    searchParameters.end_snapshot_id,
    "the latest snapshot",
  );

  return (
    <div className="flex h-full flex-col items-center justify-center gap-1 px-4 text-center text-sm text-slate-500">
      <p className="italic">
        {branchName === null
          ? "No snapshots in your selected range."
          : `Branch "${branchName}" has no snapshots in your selected range.`}
      </p>
      <p className="break-all select-text">
        Your range is from {rangeStart} to {rangeEnd}.{" "}
        {branchName === null
          ? "Try a wider range."
          : "Pick another branch or a wider range."}
      </p>
    </div>
  );
};
export default NoBranchSnapshotsNotice;
