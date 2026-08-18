import { forwardRef } from "react";
import type { CSSProperties, ReactNode } from "react";
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
  ) => (
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
          ref={scrollRef}
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
  ),
);

SidePanelFrame.displayName = "SidePanelFrame";

export default SidePanelFrame;
