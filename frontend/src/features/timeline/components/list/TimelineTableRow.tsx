import { FlexRender } from "@tanstack/react-table";
import type { Row } from "@tanstack/table-core";
import { cn } from "../../../../shared/lib/cn";
import type { TimelineRow } from "../../lib/timelineRow";
import { CELL_CLASS_BY_COLUMN } from "./columnClasses";
import type { timelineTableFeatures } from "./timelineColumns";

interface TimelineTableRowProps {
  row: Row<typeof timelineTableFeatures, TimelineRow>;
  isSelected: boolean;
  onSelect: (filePath: string) => void;
}

const TimelineTableRow = ({
  row,
  isSelected,
  onSelect,
}: TimelineTableRowProps) => {
  const isBoundary = row.original.kind === "boundary";
  const cells = row.getAllCells().map((cell) => (
    <td key={cell.id} className={CELL_CLASS_BY_COLUMN[cell.column.id]}>
      <FlexRender cell={cell} />
    </td>
  ));

  if (isBoundary) {
    return <tr>{cells}</tr>;
  }

  return (
    <tr
      tabIndex={0}
      data-file-path={row.original.filePath}
      aria-selected={isSelected}
      onClick={() => {
        onSelect(row.original.filePath);
      }}
      className={cn(
        "cursor-pointer transition-colors focus-visible:outline-none",
        isSelected ? "bg-surface" : "hover:bg-surface/60",
      )}
    >
      {cells}
    </tr>
  );
};

export default TimelineTableRow;
