import { useSyncExternalStore } from "react";
import type { KeyboardEvent, ReactNode } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { PANEL_WIDTH_MIN_REM } from "../layoutConstants";
import type { PanelAccentStyle } from "./SidePanelFrame";

const NARROW_SCREEN_QUERY = "(max-width: 767px)";
const MAIN_MIN_SIZE = "30%";
const PANEL_MAX_SIZE = "70%";
const SIDE_PANEL_MIN_SIZE = `${String(PANEL_WIDTH_MIN_REM)}rem`;
const STACKED_PANEL_DEFAULT_SIZE = "55%";
const STACKED_PANEL_MIN_SIZE = "20%";

const subscribeToNarrowScreen = (onChange: () => void): (() => void) => {
  const query = window.matchMedia(NARROW_SCREEN_QUERY);
  query.addEventListener("change", onChange);
  return () => {
    query.removeEventListener("change", onChange);
  };
};

const isNarrowScreen = (): boolean =>
  window.matchMedia(NARROW_SCREEN_QUERY).matches;

interface DockedSplitProps {
  accentColor?: string | undefined;
  className?: string;
  main: ReactNode;
  panelDefaultSize: string;
  sidePanel: ReactNode | null;
}

const DockedSplit = ({
  accentColor = "var(--color-accent)",
  className,
  main,
  panelDefaultSize,
  sidePanel,
}: DockedSplitProps) => {
  const isStacked = useSyncExternalStore(
    subscribeToNarrowScreen,
    isNarrowScreen,
  );
  const orientation = isStacked ? "vertical" : "horizontal";
  const style: PanelAccentStyle = { "--panel-accent": accentColor };

  const handleSeparatorKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (isStacked && (event.key === "ArrowUp" || event.key === "ArrowDown")) {
      event.stopPropagation();
    }
  };

  return (
    <div className={className} style={style}>
      <Group orientation={orientation}>
        <Panel id="docked-split-main" minSize={MAIN_MIN_SIZE}>
          {main}
        </Panel>
        {sidePanel !== null && (
          <>
            <Separator
              aria-label="Resize panel"
              title="Drag or use the arrow keys to resize"
              onKeyDown={handleSeparatorKeyDown}
              className="bg-(--panel-accent) transition-[filter] aria-[orientation=horizontal]:h-panel-accent aria-[orientation=vertical]:w-panel-accent data-[separator=active]:brightness-150 data-[separator=focus]:hover:brightness-150 data-[separator=hover]:brightness-150 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-slate-200"
            />
            <Panel
              key={orientation}
              id={`docked-split-${orientation}`}
              defaultSize={
                isStacked ? STACKED_PANEL_DEFAULT_SIZE : panelDefaultSize
              }
              minSize={isStacked ? STACKED_PANEL_MIN_SIZE : SIDE_PANEL_MIN_SIZE}
              maxSize={PANEL_MAX_SIZE}
            >
              {sidePanel}
            </Panel>
          </>
        )}
      </Group>
    </div>
  );
};

export default DockedSplit;
