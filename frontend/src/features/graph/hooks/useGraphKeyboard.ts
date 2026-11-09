import { useEffect } from "react";
import { isKeyboardInputTarget } from "../../../shared/lib/keyboard";
import type { NeighborDirection } from "../model/lineage";

const SCROLL_STEP_PX = 80;

export interface GraphKeyboardOptions {
  isNeighborPopupOpen: boolean;
  hasSelectedNode: boolean;
  isInspectMode: boolean;
  onToggleInspectMode: () => void;
  onCenterGraph: () => void;
  onResetView: () => void;
  onClosePanel: () => void;
  onSelectMainMetadata: () => void;
  onNavigateNeighbors: (direction: NeighborDirection) => void;
}

const isPlainKey = (event: KeyboardEvent): boolean =>
  !event.ctrlKey &&
  !event.metaKey &&
  !event.altKey &&
  !isKeyboardInputTarget(event.target);

export const useGraphKeyboard = (options: GraphKeyboardOptions): void => {
  useEffect(() => {
    const handleViewKey = (key: string): boolean => {
      if (key === "i") options.onToggleInspectMode();
      else if (key === "c") options.onCenterGraph();
      else if (key === "r") options.onResetView();
      else return false;
      return true;
    };

    const handleTraversalKey = (event: KeyboardEvent): void => {
      if (event.key === "Enter" || event.key === " ") {
        if (!options.hasSelectedNode) options.onSelectMainMetadata();
        return;
      }
      if (event.key === "h" || event.key === "ArrowLeft") {
        event.preventDefault();
        if (options.hasSelectedNode) options.onNavigateNeighbors("parents");
        return;
      }
      if (event.key === "l" || event.key === "ArrowRight") {
        event.preventDefault();
        if (options.hasSelectedNode) options.onNavigateNeighbors("children");
      }
    };

    const handleKeyDown = (event: KeyboardEvent): void => {
      if (!isPlainKey(event)) return;
      if (handleViewKey(event.key)) return;
      if (options.isNeighborPopupOpen) return;
      if (event.key === "Escape") {
        options.onClosePanel();
        return;
      }
      if (!options.isInspectMode) handleTraversalKey(event);
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [options]);
};

export const useScrollKeys = (
  isEnabled: boolean,
  onScroll: (delta: number) => void,
): void => {
  useEffect(() => {
    if (!isEnabled) return;
    const handleKeyDown = (event: KeyboardEvent): void => {
      if (!isPlainKey(event)) return;
      if (event.key === "j" || event.key === "ArrowDown") {
        event.preventDefault();
        onScroll(SCROLL_STEP_PX);
      } else if (event.key === "k" || event.key === "ArrowUp") {
        event.preventDefault();
        onScroll(-SCROLL_STEP_PX);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [isEnabled, onScroll]);
};
