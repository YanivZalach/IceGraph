import { Link } from "@tanstack/react-router";

interface TimelineEmptyStateProps {
  tableName: string | undefined;
  unreadableCommitCount: number;
}

const TimelineEmptyState = ({
  tableName,
  unreadableCommitCount,
}: TimelineEmptyStateProps) => (
  <div className="flex flex-col items-center gap-3 py-16 text-center">
    {unreadableCommitCount > 0 && (
      <p
        role="status"
        className="rounded-lg border border-amber-800 bg-amber-950/40 px-3 py-2 text-sm text-amber-300"
      >
        {unreadableCommitCount === 1
          ? "1 event could not be read."
          : `${unreadableCommitCount.toString()} events could not be read.`}
      </p>
    )}
    <p className="text-sm text-slate-400">
      {unreadableCommitCount > 0
        ? "No readable events in the loaded snapshot range."
        : "No events in the loaded snapshot range."}
    </p>
    <Link
      to="/snapshots-selection"
      search={{ table: tableName }}
      className="text-sm text-accent hover:text-accent-dark"
    >
      Choose a different range →
    </Link>
  </div>
);

export default TimelineEmptyState;
