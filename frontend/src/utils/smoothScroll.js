const scrollPos = (el) =>
  !el ? 0 : el === window ? window.scrollY : el.scrollTop;

export function bindMouseScrollHandoff(getEl, targetRef, rafRef) {
  const sync = () => {
    targetRef.current = scrollPos(getEl());
  };
  const handoff = () => {
    if (rafRef.current) {
      cancelAnimationFrame(rafRef.current);
      rafRef.current = null;
    }
    sync();
  };
  const onScroll = () => {
    if (!rafRef.current) sync();
  };

  window.addEventListener("wheel", handoff, { passive: true });
  window.addEventListener("touchmove", handoff, { passive: true });
  window.addEventListener("scroll", onScroll, { passive: true, capture: true });

  return () => {
    window.removeEventListener("wheel", handoff);
    window.removeEventListener("touchmove", handoff);
    window.removeEventListener("scroll", onScroll, { capture: true });
  };
}

export { scrollPos };
