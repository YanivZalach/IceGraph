import { useState } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { selectionAfterArrowKey } from "../lib/timelineNavigation";
import type { TimelineRow } from "../lib/timelineRow";

export interface TimelineSelection {
  selectedFilePath: string | null;
  handleSelect: (filePath: string) => void;
  clearSelection: () => void;
}

export const useTimelineSelection = (
  rows: TimelineRow[],
): TimelineSelection => {
  const [selectedFilePath, setSelectedFilePath] = useState<string | null>(null);

  const handleSelect = (filePath: string) => {
    setSelectedFilePath((current) => (current === filePath ? null : filePath));
  };

  const clearSelection = () => {
    setSelectedFilePath(null);
  };

  const handleArrowKey = (key: "ArrowDown" | "ArrowUp") => {
    const targetFilePath = selectionAfterArrowKey(rows, selectedFilePath, key);
    setSelectedFilePath(targetFilePath);
    if (targetFilePath === null) {
      return;
    }
    const targetButton = document.querySelector(
      `[data-file-path="${CSS.escape(targetFilePath)}"]`,
    );
    if (targetButton instanceof HTMLElement) {
      targetButton.focus({ preventScroll: true });
      targetButton.scrollIntoView({ block: "nearest" });
    }
  };

  useHotkey("Escape", clearSelection, {
    enabled: selectedFilePath !== null,
    stopPropagation: false,
    preventDefault: false,
  });
  useHotkey("ArrowDown", () => {
    handleArrowKey("ArrowDown");
  });
  useHotkey("ArrowUp", () => {
    handleArrowKey("ArrowUp");
  });

  return { selectedFilePath, handleSelect, clearSelection };
};
