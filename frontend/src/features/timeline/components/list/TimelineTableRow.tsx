import { FlexRender } from "@tanstack/react-table";
import type { Row } from "@tanstack/react-table";
import { cn } from "../../../../shared/lib/cn";
import type { TimelineRow } from "../../lib/timelineRow";
import {
  CELL_CLASS_BY_COLUMN,
  COLUMN_COUNT,
  RAIL_CELL_CLASS,
} from "./columnClasses";
import EventRail from "./EventRail";
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

  const gapCount = row.original.unreadableCommitsBefore;
  const gapRow = gapCount > 0 && (
    <tr>
      <td className={RAIL_CELL_CLASS}>
        <EventRail marker="gap" />
      </td>
      <td
        colSpan={COLUMN_COUNT - 1}
        className="py-4 text-lg text-amber-400"
        title={row.original.unreadableFilesBefore
          .map((file) => file.path)
          .join("\n")}
      >
        {gapCount === 1
          ? "1 event could not be read"
          : `${gapCount.toString()} events could not be read`}
      </td>
    </tr>
  );

  if (isBoundary) {
    return (
      <>
        <tr>{cells}</tr>
        {gapRow}
      </>
    );
  }

  return (
    <>
      <tr
        tabIndex={0}
        data-file-path={row.original.filePath}
        aria-current={isSelected ? "true" : undefined}
        onClick={() => {
          onSelect(row.original.filePath);
        }}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onSelect(row.original.filePath);
          }
        }}
        className={cn(
          "cursor-pointer transition-colors focus-visible:outline-2 focus-visible:-outline-offset-2 focus-visible:outline-accent",
          isSelected ? "bg-accent-muted/60" : "hover:bg-surface-hover",
        )}
      >
        {cells}
      </tr>
      {gapRow}
    </>
  );
};

export default TimelineTableRow;
