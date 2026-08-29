import { useEffect, useRef } from "react";

interface HorizontalResizeBounds {
  maximumWidthPx: number;
  minimumWidthPx: number;
  startWidthPx: number;
}

interface ResizePointerEvent {
  button: number;
  clientX: number;
  preventDefault: () => void;
}

const PRIMARY_BUTTON = 0;

const startHorizontalResize = (
  event: ResizePointerEvent,
  bounds: HorizontalResizeBounds,
  onWidthChange: (widthPx: number) => void,
): (() => void) => {
  event.preventDefault();
  const startX = event.clientX;
  const maximumWidthPx = Math.max(bounds.minimumWidthPx, bounds.maximumWidthPx);
  const controller = new AbortController();

  const finishResize = () => {
    if (controller.signal.aborted) return;
    controller.abort();
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  };

  const handleMove = (moveEvent: MouseEvent) => {
    if (moveEvent.buttons === 0) {
      finishResize();
      return;
    }
    onWidthChange(
      Math.min(
        maximumWidthPx,
        Math.max(
          bounds.minimumWidthPx,
          bounds.startWidthPx + startX - moveEvent.clientX,
        ),
      ),
    );
  };

  document.body.style.cursor = "ew-resize";
  document.body.style.userSelect = "none";
  const { signal } = controller;
  document.addEventListener("mousemove", handleMove, { signal });
  document.addEventListener("mouseup", finishResize, { signal });
  window.addEventListener("blur", finishResize, { signal });

  return finishResize;
};

export const useHorizontalResize = (
  onWidthChange: (widthPx: number) => void,
): ((event: ResizePointerEvent, bounds: HorizontalResizeBounds) => void) => {
  const cancelResizeRef = useRef<(() => void) | null>(null);

  useEffect(
    () => () => {
      cancelResizeRef.current?.();
    },
    [],
  );

  return (event, bounds) => {
    if (event.button !== PRIMARY_BUTTON) return;
    cancelResizeRef.current?.();
    cancelResizeRef.current = startHorizontalResize(
      event,
      bounds,
      onWidthChange,
    );
  };
};
