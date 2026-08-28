interface HorizontalResizeBounds {
  maximumWidthPx: number;
  minimumWidthPx: number;
  startWidthPx: number;
}

interface ResizePointerEvent {
  clientX: number;
  preventDefault: () => void;
}

export const startHorizontalResize = (
  event: ResizePointerEvent,
  bounds: HorizontalResizeBounds,
  onWidthChange: (widthPx: number) => void,
): (() => void) => {
  event.preventDefault();
  const startX = event.clientX;
  const maximumWidthPx = Math.max(bounds.minimumWidthPx, bounds.maximumWidthPx);
  const controller = new AbortController();

  const finishResize = () => {
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
