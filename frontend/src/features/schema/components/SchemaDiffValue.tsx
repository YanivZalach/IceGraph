import { cn } from "../../../shared/lib/cn";
import type { SchemaDiffStatus } from "../schemaDiff";

interface SchemaDiffValueProps {
  before: string;
  after: string;
  status: SchemaDiffStatus;
}

const SchemaDiffValue = ({ before, after, status }: SchemaDiffValueProps) => {
  const hasChanged = before !== after;

  return (
    <span className="inline-flex flex-wrap items-center gap-2 font-mono text-xs">
      {status === "added" ? (
        <span className="font-mono text-green-400">{after}</span>
      ) : status === "removed" ? (
        <span className="font-mono text-red-400 line-through">{before}</span>
      ) : hasChanged ? (
        <>
          <span className="font-mono text-red-400 line-through">{before}</span>
          <span className="text-slate-500">→</span>
          <span className="font-mono text-green-400">{after}</span>
        </>
      ) : (
        <span
          className={cn(
            "font-mono text-slate-400",
            status === "unchanged" && "opacity-60",
          )}
        >
          {after}
        </span>
      )}
    </span>
  );
};

export default SchemaDiffValue;
