import { forwardRef, useEffect, useRef, useState } from "react";
import type { CSSProperties, MouseEvent, ReactNode } from "react";
import {
  PANEL_GUTTER_REM,
  PANEL_WIDTH_DEFAULT_REM,
  PANEL_WIDTH_MIN_REM,
  pxToRem,
  remToPx,
} from "../layoutConstants";
import SidePanelFrame, { SidePanelResizeHandle } from "./SidePanelFrame";
import { isKeyboardInputTarget } from "../shared/lib/keyboard";

export interface ResizableSidePanelLayout {
  isFullscreen: boolean;
  panelWidthRem: number;
}

interface ResizableSidePanelProps {
  accentColor: string;
  header: ReactNode;
  children: ReactNode;
  onClose: () => void;
  onLayoutChange?: (layout: ResizableSidePanelLayout) => void;
  maxContainerWidth?: number;
}

interface ResizableSidePanelStyle extends CSSProperties {
  "--panel-accent": string;
}

const FullscreenToggleIcon = ({ isCompress }: { isCompress: boolean }) => (
  <svg
    className="size-3.5"
    viewBox="0 0 16 16"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.8"
  >
    <path
      d={
        isCompress
          ? "M6 2v4H2M10 2v4h4M6 14v-4H2M10 14v-4h4"
          : "M2 6V2h4M10 2h4v4M2 10v4h4M14 10v4h-4"
      }
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

const defaultMaxContainerWidth = (): number =>
  window.innerWidth - remToPx(PANEL_GUTTER_REM);

const ResizableSidePanel = forwardRef<HTMLDivElement, ResizableSidePanelProps>(
  (
    {
      accentColor,
      header,
      children,
      onClose,
      onLayoutChange,
      maxContainerWidth,
    },
    scrollRef,
  ) => {
    const [panelWidthRem, setPanelWidthRem] = useState(PANEL_WIDTH_DEFAULT_REM);
    const [isFullscreen, setIsFullscreen] = useState(false);
    const stopResizeRef = useRef<(() => void) | null>(null);

    const handleClose = () => {
      setIsFullscreen(false);
      onClose();
    };

    const startResize = (event: MouseEvent<HTMLDivElement>) => {
      event.preventDefault();
      stopResizeRef.current?.();
      const startX = event.clientX;
      const maxWidthRem = Math.max(
        PANEL_WIDTH_MIN_REM,
        pxToRem(maxContainerWidth ?? defaultMaxContainerWidth()),
      );
      const startWidthRem = Math.min(panelWidthRem, maxWidthRem);
      const { cursor, userSelect } = document.body.style;

      const handleMove = (moveEvent: globalThis.MouseEvent) => {
        setPanelWidthRem(
          Math.min(
            maxWidthRem,
            Math.max(
              PANEL_WIDTH_MIN_REM,
              startWidthRem + pxToRem(startX - moveEvent.clientX),
            ),
          ),
        );
      };

      const handleUp = () => {
        document.removeEventListener("mousemove", handleMove);
        document.removeEventListener("mouseup", handleUp);
        document.body.style.cursor = cursor;
        document.body.style.userSelect = userSelect;
        stopResizeRef.current = null;
      };

      document.body.style.cursor = "ew-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", handleMove);
      document.addEventListener("mouseup", handleUp);
      stopResizeRef.current = handleUp;
    };

    useEffect(
      () => () => {
        stopResizeRef.current?.();
      },
      [],
    );

    useEffect(() => {
      onLayoutChange?.({ isFullscreen, panelWidthRem });
    }, [isFullscreen, panelWidthRem, onLayoutChange]);

    useEffect(() => {
      const handleKeyDown = (event: KeyboardEvent) => {
        if (event.ctrlKey || event.metaKey || event.altKey) return;
        if (isKeyboardInputTarget(event.target)) return;
        if (event.key === "f" || event.key === "F") {
          event.preventDefault();
          setIsFullscreen((isCurrentlyFullscreen) => !isCurrentlyFullscreen);
        } else if (event.key === "Escape" && isFullscreen) {
          event.stopPropagation();
          setIsFullscreen(false);
        }
      };
      window.addEventListener("keydown", handleKeyDown, true);
      return () => {
        window.removeEventListener("keydown", handleKeyDown, true);
      };
    }, [isFullscreen]);

    const panelStyle: ResizableSidePanelStyle = {
      borderLeftColor: isFullscreen ? accentColor : undefined,
      width: isFullscreen ? undefined : `${String(panelWidthRem)}rem`,
      "--panel-accent": accentColor,
    };

    return (
      <SidePanelFrame
        ref={scrollRef}
        variant="floating"
        className={
          isFullscreen
            ? "fixed bottom-0 left-0 right-0 top-nav border-l-4"
            : "max-h-panel max-w-panel absolute right-4 top-4 rounded-xl"
        }
        style={panelStyle}
        header={header}
        headerActions={
          <button
            type="button"
            className="flex size-7 cursor-pointer items-center justify-center rounded-full bg-edge text-slate-400 transition hover:bg-edge-hover hover:text-slate-200"
            onClick={() => {
              setIsFullscreen(
                (isCurrentlyFullscreen) => !isCurrentlyFullscreen,
              );
            }}
            onMouseDown={(event) => {
              event.preventDefault();
            }}
            title={isFullscreen ? "Exit fullscreen" : "Fullscreen"}
          >
            <FullscreenToggleIcon isCompress={isFullscreen} />
          </button>
        }
        headerClassName={
          isFullscreen ? "px-5 pb-4 pt-5" : "pb-4 pl-9 pr-5 pt-5"
        }
        contentClassName={isFullscreen ? "px-5" : "pl-9 pr-5"}
        leading={
          isFullscreen ? null : (
            <SidePanelResizeHandle
              accentColor={accentColor}
              onMouseDown={startResize}
              title="Drag left to widen"
            />
          )
        }
        onClose={handleClose}
      >
        {children}
      </SidePanelFrame>
    );
  },
);

ResizableSidePanel.displayName = "ResizableSidePanel";

export default ResizableSidePanel;
