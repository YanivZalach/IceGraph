import { cn } from "../shared/lib/cn";
import GraphCollectionStageIcon from "./GraphCollectionStageIcon";
import type { GraphCollectionStageStatus } from "./GraphCollectionStageIcon";

const STAGE_STATUS_LABELS = {
  pending: "pending",
  in_progress: "in progress",
  done: "complete",
} as const;

interface GraphCollectionChecklistProps {
  stages: Record<string, GraphCollectionStageStatus> | null;
}

const GraphCollectionChecklist = ({
  stages,
}: GraphCollectionChecklistProps) => {
  const stageEntries = stages ? Object.entries(stages) : [];
  const completedStageCount = stageEntries.filter(
    ([, status]) => status === "done",
  ).length;

  if (!stages) {
    return (
      <div className="flex items-center gap-3 text-sm text-slate-400">
        <GraphCollectionStageIcon status="in_progress" />
        <span>Starting collection…</span>
      </div>
    );
  }

  return (
    <div className="w-full" role="status" aria-live="polite">
      <div className="mb-5 flex gap-1" aria-hidden="true">
        {stageEntries.map(([name, status]) => (
          <span
            key={name}
            className={cn(
              "h-1 flex-1 rounded-full transition-colors duration-500",
              status === "done"
                ? "bg-accent"
                : status === "in_progress"
                  ? "animate-pulse bg-accent/50 motion-reduce:animate-none"
                  : "bg-edge",
            )}
          />
        ))}
      </div>
      <ul className="flex flex-col gap-3">
        {stageEntries.map(([name, status]) => (
          <li key={name} className="flex items-center gap-3 text-sm">
            <GraphCollectionStageIcon status={status} />
            <span
              className={
                status === "in_progress"
                  ? "font-medium text-ink"
                  : status === "done"
                    ? "text-slate-500"
                    : "text-slate-600"
              }
            >
              {name}
            </span>
            <span className="sr-only">: {STAGE_STATUS_LABELS[status]}</span>
          </li>
        ))}
      </ul>
      <p className="mt-5 text-xs text-slate-500">
        {completedStageCount} of {stageEntries.length} steps complete
      </p>
    </div>
  );
};

export default GraphCollectionChecklist;
