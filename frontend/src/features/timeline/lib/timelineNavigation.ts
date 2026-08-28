import type { TimelineRow } from "./timelineRow";

const STEP_BY_KEY = { ArrowDown: 1, ArrowUp: -1 } as const;

export const selectionAfterArrowKey = (
  rows: TimelineRow[],
  selectedFilePath: string | null,
  key: "ArrowDown" | "ArrowUp",
): string | null => {
  const selectableRows = rows.filter((row) => row.kind !== "boundary");

  const newestRow = selectableRows[0];
  const isTimelineEmpty = newestRow === undefined;
  if (isTimelineEmpty) {
    return null;
  }

  const selectedIndex = selectableRows.findIndex(
    (row) => row.filePath === selectedFilePath,
  );
  const isNothingSelected = selectedIndex === -1;
  if (isNothingSelected) {
    return newestRow.filePath;
  }

  const neighbor = selectableRows[selectedIndex + STEP_BY_KEY[key]];
  const isSelectionAtTheEdge = neighbor === undefined;
  if (isSelectionAtTheEdge) {
    return selectedFilePath;
  }

  return neighbor.filePath;
};
