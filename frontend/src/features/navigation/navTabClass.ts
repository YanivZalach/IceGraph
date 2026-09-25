import { cn } from "../../shared/lib/cn";

export const navTabClass = (isActive: boolean): string =>
  cn(
    "whitespace-nowrap border-b-2 px-1 py-0.5 text-sm font-medium transition",
    isActive
      ? "border-accent text-white"
      : "border-transparent text-slate-400 hover:border-slate-500 hover:text-white",
  );
