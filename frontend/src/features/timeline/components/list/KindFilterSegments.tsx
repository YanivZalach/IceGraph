import { cn } from "../../../../shared/lib/cn";
import type { CommitKindFilter } from "../../lib/filterTimelineRows";

const KIND_FILTER_OPTIONS: { value: CommitKindFilter; label: string }[] = [
  { value: "all", label: "All" },
  { value: "writes", label: "Writes" },
  { value: "metadata", label: "Metadata" },
];

interface KindFilterSegmentsProps {
  kindFilter: CommitKindFilter;
  onKindFilterChange: (kind: CommitKindFilter) => void;
}

const KindFilterSegments = ({
  kindFilter,
  onKindFilterChange,
}: KindFilterSegmentsProps) => (
  <div
    role="group"
    aria-label="Filter by commit kind"
    className="flex rounded-md bg-surface p-0.5"
  >
    {KIND_FILTER_OPTIONS.map((option) => (
      <button
        key={option.value}
        type="button"
        aria-pressed={kindFilter === option.value}
        onClick={() => {
          onKindFilterChange(option.value);
        }}
        className={cn(
          "cursor-pointer rounded-md px-2.5 py-0.5 text-xs transition-colors",
          kindFilter === option.value
            ? "bg-canvas text-ink"
            : "text-slate-400 hover:text-slate-200",
        )}
      >
        {option.label}
      </button>
    ))}
  </div>
);

export default KindFilterSegments;
