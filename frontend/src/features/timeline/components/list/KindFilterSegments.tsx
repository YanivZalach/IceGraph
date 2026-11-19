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
    aria-label="Filter by event kind"
    className="flex overflow-hidden rounded-lg border border-edge"
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
          "cursor-pointer border-edge px-4 py-2 text-lg transition-colors not-first:border-l focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent",
          kindFilter === option.value
            ? "bg-accent text-white focus-visible:outline-white"
            : "bg-canvas text-slate-400 hover:text-ink",
        )}
      >
        {option.label}
      </button>
    ))}
  </div>
);

export default KindFilterSegments;
