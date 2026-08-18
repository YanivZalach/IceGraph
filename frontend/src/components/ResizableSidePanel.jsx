import { forwardRef, useCallback, useEffect, useState } from "react";
import {
  PANEL_ACCENT_BORDER_REM,
  PANEL_GUTTER_REM,
  PANEL_WIDTH_DEFAULT_REM,
  PANEL_WIDTH_MIN_REM,
  pxToRem,
  remToPx,
} from "../layoutConstants";
import SidePanelFrame from "./SidePanelFrame";

export { PANEL_WIDTH_RELAXED_REM as PANEL_WIDTH_RELAXED } from "../layoutConstants";

function FullscreenToggleIcon({ compress }) {
  if (compress) {
    return (
      <svg
        className="size-3.5"
        viewBox="0 0 16 16"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.8"
      >
        <path
          d="M6 2v4H2M10 2v4h4M6 14v-4H2M10 14v-4h4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    );
  }
  return (
    <svg
      className="size-3.5"
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
    >
      <path
        d="M2 6V2h4M10 2h4v4M2 10v4h4M14 10v4h-4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

const ResizableSidePanel = forwardRef(function ResizableSidePanel(
  {
    accentColor,
    header,
    children,
    onClose,
    onLayoutChange,
    maxContainerWidth = typeof window !== "undefined"
      ? remToPx(window.innerWidth) - remToPx(PANEL_GUTTER_REM)
      : remToPx(PANEL_WIDTH_DEFAULT_REM),
  },
  scrollRef,
) {
  const [panelWidthRem, setPanelWidthRem] = useState(PANEL_WIDTH_DEFAULT_REM);
  const [isFullscreen, setIsFullscreen] = useState(false);

  const handleClose = useCallback(() => {
    setIsFullscreen(false);
    onClose();
  }, [onClose]);

  const startResize = useCallback(
    (e) => {
      e.preventDefault();
      const startX = e.clientX;
      const startWidthRem = panelWidthRem;
      const maxWidthRem = Math.max(
        PANEL_WIDTH_MIN_REM,
        pxToRem(maxContainerWidth),
      );

      const onMove = (ev) => {
        const nextWidthRem = Math.min(
          maxWidthRem,
          Math.max(
            PANEL_WIDTH_MIN_REM,
            startWidthRem + pxToRem(startX - ev.clientX),
          ),
        );
        setPanelWidthRem(nextWidthRem);
      };

      const onUp = () => {
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };

      document.body.style.cursor = "ew-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    [panelWidthRem, maxContainerWidth],
  );

  useEffect(() => {
    onLayoutChange?.({ isFullscreen, panelWidthRem });
  }, [isFullscreen, panelWidthRem, onLayoutChange]);

  useEffect(() => {
    const onKey = (e) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      const tag = e.target.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || e.target.isContentEditable)
        return;
      if (e.key === "f" || e.key === "F") {
        e.preventDefault();
        setIsFullscreen((p) => !p);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const resizeHandle = !isFullscreen ? (
    <div
      onMouseDown={startResize}
      className="relative z-10 w-7 shrink-0 cursor-ew-resize self-stretch rounded-l-xl group"
      style={{
        borderLeft: `${PANEL_ACCENT_BORDER_REM}rem solid ${accentColor}`,
      }}
      title="Drag left to widen"
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
  ) : null;

  const fullscreenAction = (
    <button
      type="button"
      className="flex size-7 cursor-pointer items-center justify-center rounded-full bg-edge text-slate-400 transition hover:bg-edge-hover hover:text-slate-200"
      onClick={() => setIsFullscreen((p) => !p)}
      onMouseDown={(e) => e.preventDefault()}
      title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
    >
      <FullscreenToggleIcon compress={isFullscreen} />
    </button>
  );

  return (
    <SidePanelFrame
      ref={scrollRef}
      variant="floating"
      className={`${
        isFullscreen
          ? "fixed bottom-0 left-0 right-0 top-nav border-l-4"
          : "max-h-panel max-w-panel absolute right-4 top-4 rounded-xl"
      }`}
      style={{
        borderLeftColor: isFullscreen ? accentColor : undefined,
        width: isFullscreen ? undefined : `${panelWidthRem}rem`,
        "--panel-accent": accentColor,
      }}
      header={header}
      headerActions={fullscreenAction}
      headerClassName={isFullscreen ? "px-5 pb-4 pt-5" : "pb-4 pl-9 pr-5 pt-5"}
      contentClassName={isFullscreen ? "px-5" : "pl-9 pr-5"}
      leading={resizeHandle}
      onClose={handleClose}
    >
      {children}
    </SidePanelFrame>
  );
});

export default ResizableSidePanel;
