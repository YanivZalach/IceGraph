import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";

interface MetadataHelpProps {
  label: string;
  children: ReactNode;
}

const MetadataHelp = ({ label, children }: MetadataHelpProps) => {
  const id = useId();
  const buttonRef = useRef<HTMLButtonElement>(null);
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
    setPosition({
      left: Math.max(8, Math.min(bounds.left, window.innerWidth - 272)),
      top: bounds.bottom + 6,
    });
  };
  const hide = (): void => {
    cancelHide();
    hideTimer.current = setTimeout(() => {
      setPosition(null);
    }, 120);
  };
  const isOpen = position !== null;
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
export default MetadataHelp;
