import { cn } from "../../../shared/lib/cn";
import { RAIL_COLUMN_CLASS } from "./rowColumns";

interface EventRailProps {
  node: "current" | "plain" | "none";
}

const EventRail = ({ node }: EventRailProps) => (
  <span className={RAIL_COLUMN_CLASS}>
    <span className="absolute inset-y-0 left-1/2 w-px -translate-x-1/2 bg-edge" />
    {node !== "none" && (
      <span
        className={cn(
          "absolute top-1/2 left-1/2 size-2 -translate-x-1/2 -translate-y-1/2 rounded-full border",
          node === "current"
            ? "border-accent bg-accent ring-2 ring-accent/30"
            : "border-slate-500 bg-canvas",
        )}
      />
    )}
  </span>
);

export default EventRail;
