import { forwardRef, useCallback, useEffect, useState } from "react";
import {
  PANEL_GUTTER_REM,
  PANEL_WIDTH_DEFAULT_REM,
  PANEL_WIDTH_MIN_REM,
  pxToRem,
  remToPx,
} from "../layoutConstants";
import { startHorizontalResize } from "../shared/lib/horizontalResize";
import SidePanelFrame, { SidePanelResizeHandle } from "./SidePanelFrame";

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
      ? window.innerWidth - remToPx(PANEL_GUTTER_REM)
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

  const handleResizeStart = useCallback(
    (event) => {
      startHorizontalResize(
        event,
        {
          maximumWidthPx: maxContainerWidth,
          minimumWidthPx: remToPx(PANEL_WIDTH_MIN_REM),
          startWidthPx: remToPx(panelWidthRem),
        },
        (widthPx) => setPanelWidthRem(pxToRem(widthPx)),
      );
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
    <SidePanelResizeHandle
      accentColor={accentColor}
      onMouseDown={handleResizeStart}
      title="Drag left to widen"
    />
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
