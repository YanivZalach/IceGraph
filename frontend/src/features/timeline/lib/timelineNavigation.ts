import type { TimelineRow } from "./timelineRow";

const STEP_BY_KEY = { ArrowDown: 1, ArrowUp: -1 } as const;

export const selectionAfterArrowKey = (
  rows: TimelineRow[],
  selectedFilePath: string | null,
  rememberedFilePath: string | null,
  key: "ArrowDown" | "ArrowUp",
): string | null => {
  const newestRow = rows[0];
  const isTimelineEmpty = newestRow === undefined;
  if (isTimelineEmpty) {
    return null;
  }

  const selectedIndex = rows.findIndex(
    (row) => row.filePath === selectedFilePath,
  );
  const isNothingSelected = selectedIndex === -1;
  if (isNothingSelected) {
    const rememberedRow = rows.find(
      (row) => row.filePath === rememberedFilePath,
    );
    return rememberedRow === undefined
      ? newestRow.filePath
      : rememberedRow.filePath;
  }

  const neighbor = rows[selectedIndex + STEP_BY_KEY[key]];
  const isSelectionAtTheEdge = neighbor === undefined;
  if (isSelectionAtTheEdge) {
    return selectedFilePath;
  }

  return neighbor.filePath;
};
