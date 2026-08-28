import { Link } from "@tanstack/react-router";

interface TimelineEmptyStateProps {
  tableName: string | undefined;
}

const TimelineEmptyState = ({ tableName }: TimelineEmptyStateProps) => (
  <div className="flex flex-col items-center gap-3 py-16 text-center">
    <p className="text-sm text-slate-400">
      No commits in the loaded snapshot range.
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
