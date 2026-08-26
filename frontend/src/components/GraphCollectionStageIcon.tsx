export type GraphCollectionStageStatus = "pending" | "in_progress" | "done";

interface GraphCollectionStageIconProps {
  status: GraphCollectionStageStatus;
}

const GraphCollectionStageIcon = ({
  status,
}: GraphCollectionStageIconProps) => {
  if (status === "done") {
    return (
      <span className="flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-accent text-xs font-bold text-white">
        ✓
      </span>
    );
  }

  if (status === "in_progress") {
    return (
      <span className="relative flex h-5 w-5 shrink-0 items-center justify-center">
        <span className="absolute inset-0 animate-spin rounded-full border-2 border-accent/25 border-t-accent motion-reduce:animate-none" />
        <span className="h-1.5 w-1.5 rounded-full bg-accent" />
      </span>
    );
  }

  return (
    <span className="h-5 w-5 shrink-0 rounded-full border-2 border-edge" />
  );
};

export default GraphCollectionStageIcon;
