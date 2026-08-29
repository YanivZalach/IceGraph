import { cn } from "../../../../shared/lib/cn";
import type { TimelineRow } from "../../lib/timelineRow";
import { CHIP_BASE_CLASS } from "../Chip";
import type { BranchAccent } from "../eventColor";
import RefBadge from "./RefBadge";

interface CommitCellProps {
  row: TimelineRow;
  branchAccents: ReadonlyMap<string, BranchAccent>;
}

const CommitCell = ({ row, branchAccents }: CommitCellProps) => {
  if (row.kind === "boundary") {
    return (
      <span className="text-xs text-slate-500 italic">
        start of loaded range
      </span>
    );
  }

  const accent =
    row.branchName === null ? undefined : branchAccents.get(row.branchName);

  return (
    <span className="flex items-center gap-2">
      <span className="max-w-72 truncate text-sm text-ink">{row.title}</span>
      {accent !== undefined && (
        <span className={cn(CHIP_BASE_CLASS, accent.chip)}>
          {row.branchName}
        </span>
      )}
      {row.badges.map((badge) => (
        <RefBadge key={badge.name} name={badge.name} type={badge.type} />
      ))}
    </span>
  );
};

export default CommitCell;
