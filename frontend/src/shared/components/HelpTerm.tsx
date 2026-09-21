import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { createPortal } from "react-dom";

interface HelpTermProps {
  label: string;
  children: ReactNode;
}

const tooltipTop = (bounds: DOMRect, tooltipHeight: number): number =>
  tooltipHeight > 0 &&
  bounds.bottom + 6 + tooltipHeight > window.innerHeight &&
  bounds.top >= tooltipHeight + 6
    ? bounds.top - tooltipHeight - 6
    : bounds.bottom + 6;

const HelpTerm = ({ label, children }: HelpTermProps) => {
  const id = useId();
  const buttonRef = useRef<HTMLButtonElement>(null);
  const tooltipRef = useRef<HTMLSpanElement>(null);
  const hideTimer = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const [position, setPosition] = useState<{
    left: number;
    top: number;
  } | null>(null);
  const cancelHide = (): void => {
    clearTimeout(hideTimer.current);
  };
  const show = (): void => {
    cancelHide();
    const bounds = buttonRef.current?.getBoundingClientRect();
    if (!bounds) return;
    const tooltipHeight =
      tooltipRef.current?.getBoundingClientRect().height ?? 0;
    setPosition({
      left: Math.max(8, Math.min(bounds.left, window.innerWidth - 272)),
      top: tooltipTop(bounds, tooltipHeight),
    });
  };
  const hide = (): void => {
    cancelHide();
    hideTimer.current = setTimeout(() => {
      setPosition(null);
    }, 120);
  };
  const isOpen = position !== null;
  useLayoutEffect(() => {
    if (!isOpen) return;
    const buttonBounds = buttonRef.current?.getBoundingClientRect();
    const tooltipBounds = tooltipRef.current?.getBoundingClientRect();
    if (!buttonBounds || !tooltipBounds) return;
    const top = tooltipTop(buttonBounds, tooltipBounds.height);
    setPosition((current) =>
      current === null || current.top === top ? current : { ...current, top },
    );
  }, [isOpen]);
  useEffect(() => {
    if (!isOpen) return;
    const close = (): void => {
      setPosition(null);
    };
    window.addEventListener("scroll", close, true);
    window.addEventListener("resize", close);
    return () => {
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("resize", close);
      clearTimeout(hideTimer.current);
    };
  }, [isOpen]);
  return (
    <>
      <button
        ref={buttonRef}
        type="button"
        aria-describedby={id}
        aria-expanded={isOpen}
        className="cursor-help border-b border-dotted border-slate-500 text-left normal-case tracking-normal focus-visible:outline-2 focus-visible:outline-accent"
        onMouseEnter={show}
        onMouseLeave={hide}
        onClick={show}
        onFocus={show}
        onBlur={hide}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.stopPropagation();
            cancelHide();
            setPosition(null);
          }
        }}
      >
        {label}
      </button>
      {createPortal(
        <span
          ref={tooltipRef}
          id={id}
          role="tooltip"
          hidden={!isOpen}
          style={{ left: position?.left, top: position?.top }}
          onMouseEnter={cancelHide}
          onMouseLeave={hide}
          className="fixed z-[10000] w-64 max-w-[75vw] rounded-lg border border-edge bg-surface-deep p-3 text-left text-xs font-normal leading-relaxed text-ink shadow-xl"
        >
          {children}
        </span>,
        document.body,
      )}
    </>
  );
};

export default HelpTerm;
