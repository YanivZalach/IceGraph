export const CELL_CLASS_BY_COLUMN: Record<string, string> = {
  rail: "relative w-8 min-w-8",
  commit: "w-px py-2 pr-3 whitespace-nowrap",
  changes: "w-full max-w-0 truncate py-2 pr-3 text-xs text-slate-400",
  snapshot:
    "w-px py-2 pr-3 text-right font-mono text-xs whitespace-nowrap text-slate-500",
  time: "w-px py-2 pr-3 text-right text-xs whitespace-nowrap text-slate-500",
};

export const HEADER_CLASS_BY_COLUMN: Record<string, string> = {
  rail: "w-8 min-w-8",
  commit: "pr-3 pb-2 text-left",
  changes: "pr-3 pb-2 text-left",
  snapshot: "pr-3 pb-2 text-right",
  time: "pr-3 pb-2 text-right",
};
