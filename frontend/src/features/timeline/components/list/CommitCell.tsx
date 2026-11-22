import { cn } from "../../../../shared/lib/cn";
import { titleOf, type TimelineRow } from "../../lib/timelineRow";
import { CHIP_BASE_CLASS } from "../Chip";
import { eventToneFor, type BranchAccent } from "../eventColor";

interface CommitCellProps {
  row: TimelineRow;
  branchAccents: ReadonlyMap<string, BranchAccent>;
}

const CommitCell = ({ row, branchAccents }: CommitCellProps) => {
  if (row.kind === "gap") {
    return (
      <span className={cn("text-sm font-medium", eventToneFor(row).textClass)}>
        {titleOf(row)}
      </span>
    );
  }

  if (row.movedToBranchName !== null) {
    return (
      <span className="flex items-center gap-2">
        <span className={cn(CHIP_BASE_CLASS, branchAccents.get("main")?.chip)}>
          main
        </span>
        <span
          className={cn("text-sm font-medium", eventToneFor(row).textClass)}
        >
          moved to
        </span>
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
      <span
        className={cn(
          "max-w-72 truncate text-sm font-medium",
          eventToneFor(row).textClass,
        )}
      >
        {row.title}
      </span>
      {accent !== undefined && (
        <span className={cn(CHIP_BASE_CLASS, accent.chip)}>{branchName}</span>
      )}
    </span>
  );
};

export default CommitCell;
