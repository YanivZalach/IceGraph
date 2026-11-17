export const CELL_CLASS_BY_COLUMN: Record<string, string> = {
  rail: "relative w-10 min-w-10",
  commit: "w-px py-3.5 pr-6 whitespace-nowrap",
  changes:
    "w-full max-w-0 min-w-48 truncate py-3.5 pr-6 text-xs leading-5 text-slate-400",
  snapshot:
    "w-px py-3.5 pr-6 text-right font-mono text-xs whitespace-nowrap text-slate-500",
  time: "w-px py-3.5 pr-5 text-right text-xs whitespace-nowrap text-slate-400",
};

export const HEADER_CLASS_BY_COLUMN: Record<string, string> = {
  rail: "w-10 min-w-10",
  commit: "py-3 pr-6 text-left",
  changes: "py-3 pr-6 text-left",
  snapshot: "py-3 pr-6 text-right",
  time: "py-3 pr-5 text-right",
};
