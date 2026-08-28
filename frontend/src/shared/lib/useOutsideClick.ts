import { useEffect } from "react";
import type { RefObject } from "react";

export const useOutsideClick = (
  containerRef: RefObject<HTMLElement | null>,
  onOutsideClick: () => void,
  isEnabled: boolean,
): void => {
  useEffect(() => {
    if (!isEnabled) return;
    const handleDocumentMouseDown = (event: MouseEvent) => {
      const isInside =
        event.target instanceof Node &&
        containerRef.current?.contains(event.target) === true;
      if (!isInside) onOutsideClick();
    };
    document.addEventListener("mousedown", handleDocumentMouseDown);
    return () => {
      document.removeEventListener("mousedown", handleDocumentMouseDown);
    };
  }, [containerRef, isEnabled, onOutsideClick]);
};
