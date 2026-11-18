import { cn } from "../../../../shared/lib/cn";

interface BranchFilterSelectProps {
  branchOptions: string[];
  branchFilter: string | null;
  onBranchFilterChange: (branchName: string | null) => void;
}

const BranchFilterSelect = ({
  branchOptions,
  branchFilter,
  onBranchFilterChange,
}: BranchFilterSelectProps) => (
  <span className="relative">
    <select
      value={branchFilter ?? ""}
      onChange={(event) => {
        onBranchFilterChange(
          event.target.value === "" ? null : event.target.value,
        );
      }}
      aria-label="Filter by branch"
      className={cn(
        "cursor-pointer appearance-none rounded-lg border border-edge bg-surface py-2 pr-9 pl-4 text-lg focus-visible:outline-2 focus-visible:outline-accent",
        branchFilter === null ? "text-slate-400" : "text-ink",
      )}
    >
      <option value="">all branches</option>
      {branchOptions.map((branchName) => (
        <option key={branchName} value={branchName}>
          {branchName}
        </option>
      ))}
    </select>
    <span className="pointer-events-none absolute top-1/2 right-2.5 -translate-y-1/2 text-lg text-slate-400">
      ▾
    </span>
  </span>
);

export default BranchFilterSelect;
