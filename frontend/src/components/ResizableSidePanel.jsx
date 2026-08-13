import { forwardRef, useCallback, useEffect, useState } from "react";
import {
  PANEL_ACCENT_BORDER_REM,
  PANEL_GUTTER_REM,
  PANEL_WIDTH_DEFAULT_REM,
  PANEL_WIDTH_MIN_REM,
  pxToRem,
  remToPx,
} from "../layoutConstants";
import { PanelHeader } from "./PanelContent";

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

  const contentPad = isFullscreen ? "px-5" : "pl-9 pr-5";

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

  return (
    <div
      className={`flex bg-surface z-[1000] shadow-xl ${
        isFullscreen
          ? "fixed top-nav left-0 right-0 bottom-0 border-l-4 overflow-hidden"
          : "absolute top-4 right-4 max-h-panel max-w-panel rounded-xl overflow-hidden"
      }`}
      style={{
        borderLeftColor: isFullscreen ? accentColor : undefined,
        width: isFullscreen ? undefined : `${panelWidthRem}rem`,
        "--panel-accent": accentColor,
      }}
    >
      {!isFullscreen && (
        <div
          onMouseDown={startResize}
          className="relative shrink-0 self-stretch w-7 cursor-ew-resize z-10 group rounded-l-xl"
          style={{
            borderLeft: `${PANEL_ACCENT_BORDER_REM}rem solid ${accentColor}`,
          }}
          title="Drag left to widen"
        >
          <div
            className="absolute inset-0 rounded-l-xl pointer-events-none transition-colors bg-transparent group-hover:bg-[color-mix(in_srgb,var(--panel-accent)_25%,transparent)] group-active:bg-[color-mix(in_srgb,var(--panel-accent)_40%,transparent)]"
            aria-hidden="true"
          />
          <div
            className="absolute left-0 top-1/2 -translate-y-1/2 w-7 flex items-center justify-center pointer-events-none text-white/85 drop-shadow-sm group-hover:text-white transition-colors"
            aria-hidden="true"
          >
            <svg
              className="w-4 h-7"
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
      )}
      <div className="flex flex-col flex-1 min-h-0 min-w-0 overflow-hidden">
        <div
          className={`flex items-start justify-between pt-5 pb-4 border-b border-edge shrink-0 ${contentPad}`}
        >
          {header}
          <div className="flex items-center gap-2 shrink-0">
            <button
              type="button"
              className="size-7 rounded-full bg-edge text-slate-400 flex items-center justify-center cursor-pointer hover:bg-edge-hover hover:text-slate-200 transition"
              onClick={() => setIsFullscreen((p) => !p)}
              onMouseDown={(e) => e.preventDefault()}
              title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
            >
              <FullscreenToggleIcon compress={isFullscreen} />
            </button>
            <button
              type="button"
              className="size-7 rounded-full bg-edge text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-edge-hover hover:text-slate-200 transition"
              onClick={handleClose}
              onMouseDown={(e) => e.preventDefault()}
              title="Close"
            >
              ✕
            </button>
          </div>
        </div>
        <div
          ref={scrollRef}
          className={`py-4 flex flex-col gap-3 overflow-y-auto flex-1 min-h-0 ${contentPad}`}
        >
          {children}
        </div>
      </div>
    </div>
  );
});

export default ResizableSidePanel;
