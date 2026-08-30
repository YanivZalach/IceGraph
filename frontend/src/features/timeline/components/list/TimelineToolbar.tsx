import type { CommitKindFilter } from "../../lib/filterTimelineRows";
import BranchFilterSelect from "./BranchFilterSelect";
import KindFilterSegments from "./KindFilterSegments";

interface TimelineToolbarProps {
  commitCount: number;
  shownCommitCount: number;
  unreadableCommitCount: number;
  kindFilter: CommitKindFilter;
  onKindFilterChange: (kind: CommitKindFilter) => void;
  branchOptions: string[];
  branchFilter: string | null;
  onBranchFilterChange: (branchName: string | null) => void;
}

const TimelineToolbar = ({
  commitCount,
  shownCommitCount,
  unreadableCommitCount,
  kindFilter,
  onKindFilterChange,
  branchOptions,
  branchFilter,
  onBranchFilterChange,
}: TimelineToolbarProps) => {
  const isFiltering = kindFilter !== "all" || branchFilter !== null;
  const commitsWord = commitCount === 1 ? "commit" : "commits";
  const countText = isFiltering
    ? `${shownCommitCount.toString()} of ${commitCount.toString()} ${commitsWord}`
    : `${commitCount.toString()} ${commitsWord}`;

  return (
    <div className="shrink-0 border-b border-edge">
      <div className="mx-auto flex max-w-5xl flex-wrap items-center justify-between gap-x-4 gap-y-2 px-3 py-2">
        <p className="text-xs text-slate-400">
          {countText}
          {unreadableCommitCount > 0 && (
            <span className="text-amber-400">
              {` · ${unreadableCommitCount.toString()} could not be read`}
            </span>
          )}
        </p>

        <div className="flex items-center gap-2">
          <KindFilterSegments
            kindFilter={kindFilter}
            onKindFilterChange={onKindFilterChange}
          />
          {branchOptions.length > 0 && (
            <BranchFilterSelect
              branchOptions={branchOptions}
              branchFilter={branchFilter}
              onBranchFilterChange={onBranchFilterChange}
            />
          )}
        </div>
      </div>
    </div>
  );
};

export default TimelineToolbar;
