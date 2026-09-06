import { cn } from "../../../../shared/lib/cn";
import type { TimelineRow } from "../../lib/timelineRow";
import { CHIP_BASE_CLASS } from "../Chip";
import { displayBranchNameFor } from "../../lib/displayBranchName";
import type { BranchAccent } from "../eventColor";

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

  if (row.movedToBranchName !== null) {
    return (
      <span className="flex items-center gap-2">
        <span className={cn(CHIP_BASE_CLASS, branchAccents.get("main")?.chip)}>
          main
        </span>
        <span className="text-sm text-ink">moved to</span>
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

  const branchName = displayBranchNameFor(row);
  const accent =
    branchName === null ? undefined : branchAccents.get(branchName);

  return (
    <span className="flex items-center gap-2">
      <span className="max-w-72 truncate text-sm text-ink">{row.title}</span>
      {accent !== undefined && (
        <span className={cn(CHIP_BASE_CLASS, accent.chip)}>{branchName}</span>
      )}
    </span>
  );
};

export default CommitCell;
