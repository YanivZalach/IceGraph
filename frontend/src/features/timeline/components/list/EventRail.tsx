import { cn } from "../../../../shared/lib/cn";

export type RailMarker = "node" | "draft" | "tick" | "gap" | "none";

interface EventRailProps {
  marker: RailMarker;
  nodeClassName?: string | undefined;
}

const MARKER_POSITION_CLASS =
  "absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2";

const EventRail = ({ marker, nodeClassName }: EventRailProps) => (
  <span className="absolute inset-0" aria-hidden="true">
    <span
      className={cn(
        "absolute inset-y-0 left-1/2 -translate-x-1/2",
        marker === "gap"
          ? "border-l border-dashed border-amber-500/60"
          : "w-px bg-edge",
      )}
    />
    {(marker === "node" || marker === "draft") && (
      <span
        className={cn(
          MARKER_POSITION_CLASS,
          "size-2.5 rounded-full border-2 bg-surface",
          marker === "draft" && "border-dashed",
          nodeClassName ?? "border-accent",
        )}
      />
    )}
    {marker === "tick" && (
      <span className={cn(MARKER_POSITION_CLASS, "h-0.5 w-2.5 bg-slate-500")} />
    )}
  </span>
);

export default EventRail;
