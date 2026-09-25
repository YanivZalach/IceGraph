import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";

interface SnapshotRangeRowProps {
  label: ReactNode;
  detail?: ReactNode;
  operation?: string;
  isStart: boolean;
  isEnd: boolean;
  isInRange: boolean;
  isAnchor: boolean;
  onSelect: () => void;
}

const OPERATION_CHIP_CLASSES: Record<string, string> = {
  overwrite: "bg-blue-950/80 text-blue-400 border-blue-800",
  append: "bg-emerald-950/80 text-emerald-400 border-emerald-800",
  replace: "bg-amber-950/80 text-amber-400 border-amber-800",
  delete: "bg-rose-950/80 text-rose-400 border-rose-800",
};

const SnapshotRangeRow = ({
  label,
  detail,
  operation,
  isStart,
  isEnd,
  isInRange,
  isAnchor,
  onSelect,
}: SnapshotRangeRowProps) => {
  const isEdge = isStart || isEnd;
  return (
    <li className="relative flex gap-3">
      <div
        aria-hidden="true"
        className="relative flex w-4 shrink-0 justify-center"
      >
        <div
          className={cn(
            "absolute inset-y-0 w-0.5",
            isInRange ? "bg-accent" : "bg-edge",
          )}
        />
        <div
          className={cn(
            "relative mt-4 size-3 rounded-full border-2",
            isEdge
              ? "border-accent bg-accent"
              : isInRange
                ? "border-accent bg-surface"
                : "border-slate-600 bg-surface",
            isAnchor && "ring-4 ring-accent/30",
          )}
        />
      </div>
      <button
        type="button"
        aria-pressed={isEdge}
        onClick={onSelect}
        className={cn(
          "my-1 min-w-0 flex-1 cursor-pointer rounded-lg border px-3 py-2 text-left transition",
          isEdge
            ? "border-accent bg-accent/15"
            : isInRange
              ? "border-accent/30 bg-accent/5 hover:border-accent/60"
              : "border-edge bg-surface hover:border-edge-hover",
        )}
      >
        <div className="flex items-center gap-2">
          <div className="min-w-0 flex-1 text-sm font-semibold text-slate-200">
            {label}
          </div>
          {isStart && (
            <span className="shrink-0 rounded bg-accent px-1.5 py-0.5 text-xs font-bold text-white">
              Start
            </span>
          )}
          {isEnd && (
            <span className="shrink-0 rounded bg-accent px-1.5 py-0.5 text-xs font-bold text-white">
              End
            </span>
          )}
          {operation && (
            <span
              className={cn(
                "shrink-0 rounded border px-1.5 py-0.5 text-xs font-bold uppercase",
                OPERATION_CHIP_CLASSES[operation] ??
                  "bg-slate-800 text-slate-400 border-slate-700",
              )}
            >
              {operation}
            </span>
          )}
        </div>
        {detail && (
          <div className="mt-0.5 truncate text-xs text-slate-500">{detail}</div>
        )}
      </button>
    </li>
  );
};
export default SnapshotRangeRow;
