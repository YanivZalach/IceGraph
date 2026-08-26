type StageStatus = "pending" | "in_progress" | "done";

interface GraphCollectionChecklistProps {
  stages: Record<string, StageStatus> | null;
  stage: string | null;
}

const StageIcon = ({ status }: { status: StageStatus }) => {
  if (status === "done") {
    return (
      <span className="flex h-4 w-4 items-center justify-center rounded-full bg-accent text-[10px] text-white">
        ✓
      </span>
    );
  }

  if (status === "in_progress") {
    return (
      <span className="h-4 w-4 rounded-full border-2 border-edge border-t-accent animate-spin" />
    );
  }

  return <span className="h-4 w-4 rounded-full border-2 border-edge" />;
};

const GraphCollectionChecklist = ({
  stages,
  stage,
}: GraphCollectionChecklistProps) => {
  const collectionDone = stages
    ? Object.values(stages).every((status) => status === "done")
    : false;

  if (!stages || collectionDone) {
    return <p className="text-sm text-slate-400">{stage || "Starting…"}</p>;
  }

  return (
    <ul className="flex flex-col gap-2 w-64">
      {Object.entries(stages).map(([name, status]) => (
        <li
          key={name}
          className="flex items-center gap-2 text-sm text-slate-400"
        >
          <StageIcon status={status} />
          <span
            className={status === "done" ? "text-slate-500 line-through" : ""}
          >
            {name}
          </span>
        </li>
      ))}
    </ul>
  );
};

export default GraphCollectionChecklist;
