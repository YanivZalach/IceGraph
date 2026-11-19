import { cn } from "../../../../shared/lib/cn";
import type { TimelineRow } from "../../lib/timelineRow";
import { CHIP_BASE_CLASS } from "../Chip";
import type { BranchAccent } from "../eventColor";

interface CommitCellProps {
  row: TimelineRow;
  branchAccents: ReadonlyMap<string, BranchAccent>;
}

const CommitCell = ({ row, branchAccents }: CommitCellProps) => {
  if (row.kind === "boundary") {
    return (
      <span className="text-base text-slate-300 italic">
        start of loaded range
      </span>
    );
  }

  if (row.movedToBranchName !== null) {
    return (
      <span className="flex items-center gap-2">
        <span className={cn(CHIP_BASE_CLASS, branchAccents.get("main")?.chip)}>
          main
        </span>
        <span className="text-xl text-ink">moved to</span>
        <span
          className={cn(
            CHIP_BASE_CLASS,
            branchAccents.get(row.movedToBranchName)?.chip,
          )}
        >
          {row.movedToBranchName}
        </span>
      </span>
    );
  }

  const { branchName } = row;
  const accent =
    branchName === null ? undefined : branchAccents.get(branchName);

  return (
    <span className="flex items-center gap-2">
      <span className="max-w-[28rem] truncate text-xl font-medium text-ink">
        {row.title}
      </span>
      {accent !== undefined && (
        <span className={cn(CHIP_BASE_CLASS, accent.chip)}>{branchName}</span>
      )}
    </span>
  );
};

export default CommitCell;
