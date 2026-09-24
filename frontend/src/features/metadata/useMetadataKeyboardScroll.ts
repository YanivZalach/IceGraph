import { useHotkey } from "@tanstack/react-hotkeys";
import { isKeyboardInputTarget } from "../../shared/lib/keyboard";

const canHandleScroll = (event: KeyboardEvent): boolean => {
  const target = event.target;
  return target instanceof HTMLElement && !isKeyboardInputTarget(target);
};

const scrollFocusedRegion = (event: KeyboardEvent, delta: number): void => {
  if (!canHandleScroll(event)) return;
  const target = event.target;
  const panel =
    target instanceof HTMLElement
      ? target.closest("[data-metadata-scroll]")
      : null;
  event.preventDefault();
  if (
    panel instanceof HTMLElement &&
    (delta > 0
      ? panel.scrollTop + panel.clientHeight < panel.scrollHeight - 1
      : panel.scrollTop > 0)
  ) {
    panel.scrollBy({ top: delta, behavior: "smooth" });
    return;
  }
  window.scrollBy({ top: delta, behavior: "smooth" });
};

export const useMetadataKeyboardScroll = (isOverlayOpen: boolean): void => {
  const options = {
    enabled: !isOverlayOpen,
    preventDefault: false,
    stopPropagation: false,
  };
  useHotkey(
    "J",
    (event) => {
      scrollFocusedRegion(event, 80);
    },
    options,
  );
  useHotkey(
    "K",
    (event) => {
      scrollFocusedRegion(event, -80);
    },
    options,
  );
};
