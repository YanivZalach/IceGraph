import { useRef, type PointerEvent, type WheelEvent } from "react";

const DRAG_THRESHOLD_PX = 4;

export interface CanvasGestureHandlers {
  onWheelCapture: (event: WheelEvent<HTMLDivElement>) => void;
  onPointerDownCapture: (event: PointerEvent<HTMLDivElement>) => void;
  onPointerMoveCapture: (event: PointerEvent<HTMLDivElement>) => void;
}

export const useCanvasGesture = (
  onViewMoved: () => void,
): CanvasGestureHandlers => {
  const dragStartRef = useRef<{ x: number; y: number } | null>(null);

  return {
    onWheelCapture: (event) => {
      if (event.target instanceof HTMLCanvasElement) onViewMoved();
    },
    onPointerDownCapture: (event) => {
      dragStartRef.current =
        event.target instanceof HTMLCanvasElement
          ? { x: event.clientX, y: event.clientY }
          : null;
    },
    onPointerMoveCapture: (event) => {
      const dragStart = dragStartRef.current;
      if (dragStart === null || event.buttons === 0) return;
      const distance = Math.hypot(
        event.clientX - dragStart.x,
        event.clientY - dragStart.y,
      );
      if (distance < DRAG_THRESHOLD_PX) return;
      dragStartRef.current = null;
      onViewMoved();
    },
  };
};
