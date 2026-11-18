import { useSyncExternalStore } from "react";
import type { ReactNode } from "react";
import { Group, Panel, Separator } from "react-resizable-panels";
import { PANEL_ACCENT_BORDER_REM } from "../layoutConstants";

/** Below Tailwind's `md` breakpoint the side panel stacks under the main content. */
const NARROW_SCREEN_QUERY = "(max-width: 767px)";

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
  const dividerThickness = `${String(PANEL_ACCENT_BORDER_REM)}rem`;

  return (
    // The Group sizes itself to 100% of its parent, so this element owns the size.
    <div className={className}>
      <Group orientation={isStacked ? "vertical" : "horizontal"}>
        <Panel id="docked-split-main" minSize="30%">
          {main}
        </Panel>
        {sidePanel !== null && (
          <>
            <Separator
              aria-label="Resize panel"
              title="Drag or use the arrow keys to resize"
              // The divider owns the arrow keys while focused; page-level
              // arrow shortcuts must not also act on the same key press.
              onKeyDown={(event) => {
                if (event.key.startsWith("Arrow")) {
                  event.stopPropagation();
                }
              }}
              className="shrink-0 transition-[filter] hover:brightness-150 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-slate-200"
              style={{
                backgroundColor: accentColor,
                ...(isStacked
                  ? { height: dividerThickness }
                  : { width: dividerThickness }),
              }}
            />
            <Panel
              // A size measured for one direction is meaningless in the other,
              // so each direction is its own panel with its own default.
              key={isStacked ? "stacked" : "side-by-side"}
              id={isStacked ? "docked-split-stacked" : "docked-split-side"}
              defaultSize={isStacked ? "55%" : panelDefaultSize}
              minSize={isStacked ? "20%" : "20rem"}
              maxSize="70%"
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
