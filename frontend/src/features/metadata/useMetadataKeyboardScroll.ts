import { useEffect, useRef } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { isKeyboardInputTarget } from "../../shared/lib/keyboard";

type ScrollRegion = HTMLElement | Window;

interface ScrollState {
  region: ScrollRegion;
  target: number;
  frame: number | null;
}

const scrollPosition = (region: ScrollRegion): number =>
  region instanceof HTMLElement ? region.scrollTop : window.scrollY;

const maximumScroll = (region: ScrollRegion): number =>
  region instanceof HTMLElement
    ? region.scrollHeight - region.clientHeight
    : document.documentElement.scrollHeight - window.innerHeight;

const setScrollPosition = (region: ScrollRegion, top: number): void => {
  if (region instanceof HTMLElement) region.scrollTop = top;
  else window.scrollTo(0, top);
};

export const useMetadataKeyboardScroll = (isOverlayOpen: boolean): void => {
  const scrollState = useRef<ScrollState | null>(null);

  const scrollFocusedRegion = (event: KeyboardEvent, delta: number): void => {
    const target = event.target;
    if (!(target instanceof HTMLElement) || isKeyboardInputTarget(target))
      return;
    const panel = target.closest("[data-metadata-scroll]");
    const state = scrollState.current;
    const region =
      panel instanceof HTMLElement &&
      (delta > 0
        ? panel.scrollTop < maximumScroll(panel) - 1
        : panel.scrollTop > 0)
        ? panel
        : window;
    event.preventDefault();

    if (state && state.region !== region && state.frame !== null)
      cancelAnimationFrame(state.frame);
    const nextState =
      state?.region === region
        ? state
        : { region, target: scrollPosition(region), frame: null };
    scrollState.current = nextState;
    nextState.target = Math.max(
      0,
      Math.min(nextState.target + delta, maximumScroll(region)),
    );
    if (nextState.frame !== null) return;

    const animate = (): void => {
      const difference = nextState.target - scrollPosition(region);
      if (Math.abs(difference) < 0.5) {
        setScrollPosition(region, nextState.target);
        nextState.frame = null;
        return;
      }
      setScrollPosition(region, scrollPosition(region) + difference * 0.14);
      nextState.frame = requestAnimationFrame(animate);
    };
    nextState.frame = requestAnimationFrame(animate);
  };

  useEffect(() => {
    const cancelAnimation = (): void => {
      const state = scrollState.current;
      if (!state) return;
      if (state.frame !== null) cancelAnimationFrame(state.frame);
      state.frame = null;
      state.target = scrollPosition(state.region);
    };
    const syncScroll = (): void => {
      const state = scrollState.current;
      if (state && state.frame === null)
        state.target = scrollPosition(state.region);
    };
    window.addEventListener("wheel", cancelAnimation, { passive: true });
    window.addEventListener("touchmove", cancelAnimation, { passive: true });
    window.addEventListener("scroll", syncScroll, {
      passive: true,
      capture: true,
    });
    if (isOverlayOpen) cancelAnimation();
    return () => {
      window.removeEventListener("wheel", cancelAnimation);
      window.removeEventListener("touchmove", cancelAnimation);
      window.removeEventListener("scroll", syncScroll, { capture: true });
      cancelAnimation();
    };
  }, [isOverlayOpen]);

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
