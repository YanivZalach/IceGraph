import type { MouseEvent, ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import {
  UI_TOOLBAR_BUTTON_LAYOUT,
  toolbarButtonClass,
} from "../../../uiTypography";

interface GraphToolbarProps {
  isFullView: boolean;
  isInspectMode: boolean;
  onResetView: () => void;
  onToggleInspectMode: () => void;
  onCenterGraph: () => void;
  search: ReactNode;
  notice: ReactNode;
}

const keepFocusOnGraph = (event: MouseEvent): void => {
  event.preventDefault();
};

const GraphToolbar = ({
  isFullView,
  isInspectMode,
  onResetView,
  onToggleInspectMode,
  onCenterGraph,
  search,
  notice,
}: GraphToolbarProps) => (
  <div className="pointer-events-none absolute top-4 left-4 flex w-[min(20rem,calc(100%-2rem))] flex-col items-start gap-2 font-sans">
    {search !== null && (
      <div className="pointer-events-auto relative z-[1050] w-full">
        {search}
      </div>
    )}
    <div className="relative z-[10] flex w-52 flex-col gap-2 *:pointer-events-auto">
      <button
        type="button"
        className={toolbarButtonClass(isFullView)}
        onClick={() => {
          if (!isFullView) onResetView();
        }}
        onMouseDown={keepFocusOnGraph}
      >
        Reset Full View
      </button>

      <button
        type="button"
        className={cn(
          UI_TOOLBAR_BUTTON_LAYOUT,
          "flex overflow-hidden",
          isInspectMode
            ? "bg-accent text-white border border-accent hover:bg-accent-dark"
            : "bg-surface text-accent border border-accent hover:bg-edge",
        )}
        onClick={onToggleInspectMode}
        onMouseDown={keepFocusOnGraph}
      >
        <span className="flex w-9 shrink-0 items-center justify-center bg-black/5 py-2.5 text-lg">
          {isInspectMode ? "🔒" : "🔍"}
        </span>
        <span className="flex flex-1 items-center justify-center px-2 py-2.5 leading-tight">
          {isInspectMode ? "Inspect (Locked)" : "Lineage Traversal"}
        </span>
      </button>

      <button
        type="button"
        className={toolbarButtonClass(false)}
        onClick={onCenterGraph}
        onMouseDown={keepFocusOnGraph}
      >
        Center Graph
      </button>
    </div>
    {notice !== null && (
      <div className="pointer-events-auto relative z-[10] w-full">{notice}</div>
    )}
  </div>
);

export default GraphToolbar;
