const STAGE_PERCENT: Record<string, number> = {
  "Collecting snapshots": 20,
  "Collecting metadata, manifests and data files": 55,
  "Building graph": 85,
};

const DEFAULT_PERCENT = 5;

interface GraphProgressBarProps {
  stage: string | null;
}

const GraphProgressBar = ({ stage }: GraphProgressBarProps) => {
  const percent = (stage && STAGE_PERCENT[stage]) || DEFAULT_PERCENT;

  return (
    <div className="w-64">
      <div className="h-2 w-full rounded-full bg-edge overflow-hidden">
        <div
          className="h-full rounded-full bg-accent transition-[width] duration-700 ease-out"
          style={{ width: `${String(percent)}%` }}
        />
      </div>
      <p className="mt-3 text-center text-sm text-slate-400">
        {stage || "Starting…"}
      </p>
    </div>
  );
};

export default GraphProgressBar;
