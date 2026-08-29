import { cn } from "../../../../shared/lib/cn";

interface EventRailProps {
  marker: "node" | "tick" | "none";
  nodeClassName?: string | undefined;
}

const EventRail = ({ marker, nodeClassName }: EventRailProps) => (
  <span className="absolute inset-0">
    <span className="absolute inset-y-0 left-1/2 w-px -translate-x-1/2 bg-edge" />
    {marker === "node" && (
      <span
        className={cn(
          "absolute top-1/2 left-1/2 size-2 -translate-x-1/2 -translate-y-1/2 rounded-full border bg-canvas",
          nodeClassName ?? "border-slate-500",
        )}
      />
    )}
    {marker === "tick" && (
      <span className="absolute top-1/2 left-1/2 h-px w-2 -translate-y-1/2 bg-edge" />
    )}
  </span>
);

export default EventRail;
