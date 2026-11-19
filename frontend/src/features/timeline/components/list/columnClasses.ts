export const COLUMN_COUNT = 5;

export const RAIL_CELL_CLASS = "relative w-14 min-w-14";

export const CELL_CLASS_BY_COLUMN: Record<string, string> = {
  rail: RAIL_CELL_CLASS,
  commit: "w-px py-4 pr-8 whitespace-nowrap",
  changes:
    "w-full max-w-0 min-w-48 truncate py-4 pr-8 text-base text-slate-300",
  snapshot:
    "w-px py-4 pr-8 text-right font-mono text-sm whitespace-nowrap text-slate-500",
  time: "w-px py-4 pr-8 text-right text-sm whitespace-nowrap text-slate-400 tabular-nums",
};

export const HEADER_CLASS_BY_COLUMN: Record<string, string> = {
  rail: "w-14 min-w-14",
  commit: "py-3.5 pr-8 text-left",
  changes: "py-3.5 pr-8 text-left",
  snapshot: "py-3.5 pr-8 text-right",
  time: "py-3.5 pr-8 text-right",
};
