import { cn } from "../../../shared/lib/cn";

export interface SnapshotRowMarking {
  isEdge: boolean;
  isInRange: boolean;
  isAnchor: boolean;
  badges: readonly string[];
}

interface SnapshotRailProps {
  marking: SnapshotRowMarking;
  hasNode: boolean;
}

const SnapshotRail = ({ marking, hasNode }: SnapshotRailProps) => (
  <span className="absolute inset-0" aria-hidden="true">
    <span
      className={cn(
        "absolute inset-y-0 left-1/2 -translate-x-1/2",
        marking.isInRange ? "w-0.5 bg-accent" : "w-px bg-edge",
      )}
    />
    {hasNode && (
      <span
        className={cn(
          "absolute top-1/2 left-1/2 size-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2",
          marking.isEdge
            ? "border-accent bg-accent"
            : marking.isInRange
              ? "border-accent bg-surface"
              : "border-slate-600 bg-surface",
          marking.isAnchor && "ring-4 ring-accent/30",
        )}
      />
    )}
  </span>
);

export default SnapshotRail;
