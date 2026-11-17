import { cn } from "../../../../shared/lib/cn";
import type { DiffEntry } from "../../lib/details/describeFieldDiff";

interface DiffEntryRowProps {
  entry: DiffEntry;
}

const VALUE_CLASS =
  "min-w-0 rounded px-1.5 py-0.5 font-mono text-xs break-all whitespace-pre-wrap";
const BEFORE_CLASS = "bg-red-500/10 text-red-300";
const AFTER_CLASS = "bg-green-500/10 text-green-300";
const UNCHANGED_CLASS = "bg-canvas text-slate-300";

const STATUS_MARK = {
  added: "+",
  removed: "−",
  changed: "~",
  unchanged: "=",
} as const;

const DiffEntryRow = ({ entry }: DiffEntryRowProps) => (
  <li className="grid grid-cols-[0.75rem_minmax(0,1fr)] gap-x-1.5">
    <span
      aria-label={entry.status}
      className={cn(
        "pt-0.5 text-center font-mono text-xs",
        entry.status === "added" && "text-green-400",
        entry.status === "removed" && "text-red-400",
        entry.status === "changed" && "text-amber-300",
        entry.status === "unchanged" && "text-slate-600",
      )}
    >
      {STATUS_MARK[entry.status]}
    </span>
    <div className="flex min-w-0 flex-col gap-1">
      {entry.label !== "" && (
        <span className="font-mono text-xs break-all text-slate-400">
          {entry.label}
        </span>
      )}
      {entry.status === "unchanged" && (
        <span className={cn(VALUE_CLASS, UNCHANGED_CLASS)}>{entry.after}</span>
      )}
      {(entry.status === "changed" || entry.status === "removed") && (
        <span
          className={cn(
            VALUE_CLASS,
            BEFORE_CLASS,
            "line-through decoration-red-400/40",
          )}
        >
          {entry.before}
        </span>
      )}
      {(entry.status === "changed" || entry.status === "added") && (
        <span className={cn(VALUE_CLASS, AFTER_CLASS)}>{entry.after}</span>
      )}
    </div>
  </li>
);

export default DiffEntryRow;
