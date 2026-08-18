import { forwardRef, useRef } from "react";
import type { CSSProperties, MouseEventHandler, ReactNode } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { PANEL_ACCENT_BORDER_REM } from "../layoutConstants";
import { cn } from "../shared/lib/cn";

type SidePanelVariant = "docked" | "floating";

interface SidePanelFrameProps {
  ariaLabel?: string;
  children: ReactNode;
  className?: string;
  closeLabel?: string;
  contentClassName?: string;
  contentTestId?: string;
  header: ReactNode;
  headerActions?: ReactNode;
  headerClassName?: string;
  leading?: ReactNode;
  onClose: () => void;
  style?: CSSProperties;
  variant: SidePanelVariant;
}

interface SidePanelResizeHandleProps {
  accentColor: string;
  className?: string;
  onMouseDown: MouseEventHandler<HTMLDivElement>;
  title?: string;
}

export const SidePanelResizeHandle = ({
  accentColor,
  className,
  onMouseDown,
  title = "Drag to resize",
}: SidePanelResizeHandleProps) => (
  <div
    onMouseDown={onMouseDown}
    className={cn(
      "relative z-10 w-7 shrink-0 cursor-ew-resize self-stretch rounded-l-xl group",
      className,
    )}
    style={{
      borderLeft: `${String(PANEL_ACCENT_BORDER_REM)}rem solid ${accentColor}`,
    }}
    title={title}
  >
    <div
      className="pointer-events-none absolute inset-0 rounded-l-xl bg-transparent transition-colors group-hover:bg-[color-mix(in_srgb,var(--panel-accent)_25%,transparent)] group-active:bg-[color-mix(in_srgb,var(--panel-accent)_40%,transparent)]"
      aria-hidden="true"
    />
    <div
      className="pointer-events-none absolute left-0 top-1/2 flex w-7 -translate-y-1/2 items-center justify-center text-white/85 drop-shadow-sm transition-colors group-hover:text-white"
      aria-hidden="true"
    >
      <svg
        className="h-7 w-4"
        viewBox="0 0 12 22"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
      >
        <path
          d="M8 11H2M2 11L4.5 8M2 11L4.5 14"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
        <path
          d="M4 11h6M10 11L7.5 8M10 11L7.5 14"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </div>
  </div>
);

const SidePanelFrame = forwardRef<HTMLDivElement, SidePanelFrameProps>(
  (
    {
      ariaLabel,
      children,
      className,
      closeLabel = "Close panel",
      contentClassName,
      contentTestId,
      header,
      headerActions,
      headerClassName,
      leading,
      onClose,
      style,
      variant,
    },
    scrollRef,
  ) => {
    const contentRef = useRef<HTMLDivElement>(null);
    const setContentRef = (node: HTMLDivElement | null) => {
      contentRef.current = node;
      if (typeof scrollRef === "function") scrollRef(node);
      else if (scrollRef !== null) scrollRef.current = node;
    };
    useHotkey("J", () => {
      contentRef.current?.scrollBy({ behavior: "smooth", top: 120 });
    });
    useHotkey("K", () => {
      contentRef.current?.scrollBy({ behavior: "smooth", top: -120 });
    });

    return (
      <aside
        aria-label={ariaLabel}
        className={cn(
          "flex min-h-0 overflow-hidden bg-surface",
          variant === "floating" && "z-[1000] shadow-xl",
          className,
        )}
        style={style}
      >
        {leading}
        <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden">
          <div
            className={cn(
              "flex shrink-0 items-start justify-between border-b border-edge px-5 py-4",
              headerClassName,
            )}
          >
            {header}
            <div className="flex shrink-0 items-center gap-2">
              {headerActions}
              <button
                type="button"
                aria-label={closeLabel}
                title={closeLabel}
                onClick={onClose}
                onMouseDown={(event) => {
                  event.preventDefault();
                }}
                className="flex size-7 cursor-pointer items-center justify-center rounded-full bg-edge text-base text-slate-400 transition hover:bg-edge-hover hover:text-slate-200"
              >
                ✕
              </button>
            </div>
          </div>
          <div
            ref={setContentRef}
            data-testid={contentTestId}
            className={cn(
              "flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto px-5 py-4",
              contentClassName,
            )}
          >
            {children}
          </div>
        </div>
      </aside>
    );
  },
);

SidePanelFrame.displayName = "SidePanelFrame";

export default SidePanelFrame;
