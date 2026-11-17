import { useState } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { useTableSpecs } from "../../specs/tableSpecs";
import { selectionAfterArrowKey } from "../lib/timelineNavigation";
import type { TimelineRow } from "../lib/timelineRow";

export interface TimelineSelection {
  handleSelect: (filePath: string) => void;
  clearSelection: () => void;
}

export const useTimelineSelection = (
  rows: TimelineRow[],
  selectedFilePath: string | null,
  onSelectionChange: (filePath: string | undefined) => void,
): TimelineSelection => {
  const [rememberedFilePath, setRememberedFilePath] = useState<string | null>(
    null,
  );
  const { detailsOpen, issuesOpen } = useTableSpecs();

  const handleSelect = (filePath: string) => {
    setRememberedFilePath(filePath);
    onSelectionChange(selectedFilePath === filePath ? undefined : filePath);
  };

  const clearSelection = () => {
    setRememberedFilePath(selectedFilePath);
    onSelectionChange(undefined);
  };

  const handleArrowKey = (key: "ArrowDown" | "ArrowUp") => {
    const targetFilePath = selectionAfterArrowKey(
      rows,
      selectedFilePath,
      rememberedFilePath,
      key,
    );
    if (targetFilePath !== selectedFilePath) {
      onSelectionChange(targetFilePath ?? undefined);
    }
    if (targetFilePath === null) {
      return;
    }
    setRememberedFilePath(targetFilePath);
    const targetButton = document.querySelector(
      `[data-file-path="${CSS.escape(targetFilePath)}"]`,
    );
    if (targetButton instanceof HTMLElement) {
      targetButton.focus({ preventScroll: true });
      targetButton.scrollIntoView({ block: "nearest" });
    }
  };

  useHotkey("Escape", clearSelection, {
    enabled: selectedFilePath !== null && !detailsOpen && !issuesOpen,
    conflictBehavior: "allow",
    stopPropagation: false,
    preventDefault: false,
  });
  useHotkey(
    "ArrowDown",
    () => {
      handleArrowKey("ArrowDown");
    },
    { enabled: !detailsOpen && !issuesOpen },
  );
  useHotkey(
    "ArrowUp",
    () => {
      handleArrowKey("ArrowUp");
    },
    { enabled: !detailsOpen && !issuesOpen },
  );

  return { handleSelect, clearSelection };
};
