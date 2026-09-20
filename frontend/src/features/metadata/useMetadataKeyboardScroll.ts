import { useEffect } from "react";

// Native wheel scrolling hands off to the page at panel edges. Keyboard
// scrolling follows the focused panel and yields to the Specs overlay.
export const useMetadataKeyboardScroll = (isOverlayOpen: boolean): void => {
  useEffect(() => {
    if (isOverlayOpen) return;
    const handleKey = (event: KeyboardEvent): void => {
      if (
        event.defaultPrevented ||
        event.ctrlKey ||
        event.metaKey ||
        event.altKey ||
        event.shiftKey ||
        !["j", "k"].includes(event.key)
      )
        return;
      const target = event.target;
      if (
        !(target instanceof HTMLElement) ||
        target.isContentEditable ||
        target.closest("input, textarea, select, button, a, summary")
      )
        return;
      const panel = target.closest("[data-metadata-scroll]");
      const delta = event.key === "j" ? 80 : -80;
      if (
        panel instanceof HTMLElement &&
        (delta > 0
          ? panel.scrollTop + panel.clientHeight < panel.scrollHeight - 1
          : panel.scrollTop > 0)
      ) {
        event.preventDefault();
        panel.scrollBy({ top: delta, behavior: "smooth" });
      } else {
        event.preventDefault();
        window.scrollBy({ top: delta, behavior: "smooth" });
      }
    };
    window.addEventListener("keydown", handleKey);
    return () => {
      window.removeEventListener("keydown", handleKey);
    };
  }, [isOverlayOpen]);
};
