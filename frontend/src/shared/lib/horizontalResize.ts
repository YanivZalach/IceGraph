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
): void => {
  event.preventDefault();
  const startX = event.clientX;
  const maximumWidthPx = Math.max(bounds.minimumWidthPx, bounds.maximumWidthPx);

  const handleMove = (moveEvent: MouseEvent) => {
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
