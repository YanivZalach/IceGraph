import { cn } from "../../../shared/lib/cn";
import type { SchemaDiffStatus } from "../schemaDiff";

interface SchemaDiffValueProps {
  label: string;
  before: string;
  after: string;
  status: SchemaDiffStatus;
}

const SchemaDiffValue = ({
  label,
  before,
  after,
  status,
}: SchemaDiffValueProps) => {
  const hasChanged = before !== after;

  return (
    <div className="flex flex-wrap items-center gap-2 text-xs">
      {label.length > 0 && (
        <span className="font-bold uppercase text-slate-500">{label}</span>
      )}
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
    </div>
  );
};

export default SchemaDiffValue;
