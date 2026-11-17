import type { CommitKindFilter } from "../../lib/filterTimelineRows";
import BranchFilterSelect from "./BranchFilterSelect";
import KindFilterSegments from "./KindFilterSegments";

interface TimelineToolbarProps {
  commitCount: number;
  shownCommitCount: number;
  unreadableCommitCount: number;
  isTableMetadataUnreadable: boolean;
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
  isTableMetadataUnreadable,
  kindFilter,
  onKindFilterChange,
  branchOptions,
  branchFilter,
  onBranchFilterChange,
}: TimelineToolbarProps) => {
  const isFiltering = kindFilter !== "all" || branchFilter !== null;
  const eventsWord = commitCount === 1 ? "event" : "events";
  const countText = isFiltering
    ? `${shownCommitCount.toString()} of ${commitCount.toString()} ${eventsWord}`
    : `${commitCount.toString()} ${eventsWord}`;

  return (
    <div className="sticky top-0 z-10 border-b border-edge bg-canvas/90 backdrop-blur">
      <div className="mx-auto flex max-w-6xl flex-wrap items-center justify-between gap-x-4 gap-y-2 px-3 py-3 sm:px-5">
        <div className="min-w-0">
          <p className="text-sm text-slate-400">
            {countText}
            {unreadableCommitCount > 0 && (
              <span className="text-amber-400">
                {` · ${unreadableCommitCount.toString()} could not be read`}
              </span>
            )}
            {isTableMetadataUnreadable && (
              <span role="status" className="text-amber-400">
                {
                  " · table metadata could not be read, so schema, partition and sort changes show ids only"
                }
              </span>
            )}
          </p>
        </div>

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
