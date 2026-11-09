import { useEffect, useRef, type RefObject } from "react";
import { bindMouseScrollHandoff } from "../../../utils/smoothScroll";

const EASING_FACTOR = 0.14;
const SETTLE_DISTANCE_PX = 0.5;

export interface SmoothScroll {
  scrollBy: (delta: number) => void;
  scrollToTop: () => void;
}

export const useSmoothScroll = (
  elementRef: RefObject<HTMLElement | null>,
): SmoothScroll => {
  const targetRef = useRef(0);
  const frameRef = useRef<number | null>(null);

  useEffect(() => {
    const unbind = bindMouseScrollHandoff(
      () => elementRef.current,
      targetRef,
      frameRef,
    );
    return () => {
      unbind();
      if (frameRef.current !== null) cancelAnimationFrame(frameRef.current);
      frameRef.current = null;
    };
  }, [elementRef]);

  const scrollBy = (delta: number): void => {
    const element = elementRef.current;
    if (element === null) return;
    targetRef.current = Math.max(
      0,
      Math.min(
        targetRef.current + delta,
        element.scrollHeight - element.clientHeight,
      ),
    );
    if (frameRef.current !== null) return;

    const animate = (): void => {
      const distance = targetRef.current - element.scrollTop;
      if (Math.abs(distance) < SETTLE_DISTANCE_PX) {
        element.scrollTop = targetRef.current;
        frameRef.current = null;
        return;
      }
      element.scrollTop += distance * EASING_FACTOR;
      frameRef.current = requestAnimationFrame(animate);
    };
    frameRef.current = requestAnimationFrame(animate);
  };

  const scrollToTop = (): void => {
    targetRef.current = 0;
    if (elementRef.current !== null) elementRef.current.scrollTop = 0;
  };

  return { scrollBy, scrollToTop };
};
