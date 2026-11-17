import { useState } from "react";
import type { MouseEvent } from "react";

export interface DockedPanelWidth {
  panelWidthPx: number | null;
  startResize: (event: MouseEvent<HTMLDivElement>) => void;
}

/**
 * The handle is the panel's first child and the panel sits directly in the container it shares
 * with the content, so both widths are read from the DOM when a drag starts.
 */
export const useDockedPanelWidth = (
  minimumWidthPx: number,
  maximumContainerShare: number,
): DockedPanelWidth => {
  const [panelWidthPx, setPanelWidthPx] = useState<number | null>(null);

  const startResize = (event: MouseEvent<HTMLDivElement>) => {
    event.preventDefault();
    const panel = event.currentTarget.parentElement;
    if (panel === null) return;
    const container = panel.parentElement;
    if (container === null) return;

    const startX = event.clientX;
    const startWidth = panel.getBoundingClientRect().width;
    const maximumWidth = Math.max(
      minimumWidthPx,
      container.getBoundingClientRect().width * maximumContainerShare,
    );
    const handleMove = (moveEvent: globalThis.MouseEvent) => {
      setPanelWidthPx(
        Math.min(
          maximumWidth,
          Math.max(minimumWidthPx, startWidth + startX - moveEvent.clientX),
        ),
      );
    };
    const handleUp = () => {
      document.removeEventListener("mousemove", handleMove);
      document.removeEventListener("mouseup", handleUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };

    document.body.style.cursor = "ew-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", handleMove);
    document.addEventListener("mouseup", handleUp);
  };

  return { panelWidthPx, startResize };
};
