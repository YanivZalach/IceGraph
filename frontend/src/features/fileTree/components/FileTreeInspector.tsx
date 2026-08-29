import { useState } from "react";
import type { CSSProperties, MouseEvent } from "react";
import { PanelHeader } from "../../../components/PanelContent";
import SidePanelFrame, {
  SidePanelResizeHandle,
} from "../../../components/SidePanelFrame";
import type { InspectedFileTreeItem } from "../types";
import FileTreeInspectorContent from "./FileTreeInspectorContent";

interface FileTreeInspectorProps {
  inspectedItem: InspectedFileTreeItem;
  onClose: () => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

interface FileTreeInspectorStyle extends CSSProperties {
  "--file-tree-inspector-width": string;
  "--panel-accent": string;
}

const PANEL_ACCENT = "#2e86c1";
const PANEL_MIN_WIDTH_PX = 320;

const FileTreeInspector = ({
  inspectedItem,
  onClose,
  onViewInGraph,
}: FileTreeInspectorProps) => {
  const [panelWidthPx, setPanelWidthPx] = useState<number | null>(null);
  const title =
    inspectedItem.kind === "file"
      ? "Data file"
      : inspectedItem.kind === "partition-path"
        ? "Partition path statistics"
        : "Partition statistics";
  const subtitle =
    inspectedItem.kind === "file"
      ? inspectedItem.file.id
      : inspectedItem.kind === "partition-path"
        ? inspectedItem.partitionPathNode.path
        : inspectedItem.partition.name;
  const panelStyle: FileTreeInspectorStyle = {
    "--file-tree-inspector-width":
      panelWidthPx === null ? "38%" : `${String(panelWidthPx)}px`,
    "--panel-accent": PANEL_ACCENT,
  };

  const startResize = (event: MouseEvent<HTMLDivElement>) => {
    event.preventDefault();
    const panel = event.currentTarget.parentElement;
    if (panel === null) return;
    const container = panel.parentElement;
    if (container === null) return;

    const startX = event.clientX;
    const startWidth = panel.getBoundingClientRect().width;
    const maximumWidth = Math.max(
      PANEL_MIN_WIDTH_PX,
      container.getBoundingClientRect().width * 0.7,
    );
    const handleMove = (moveEvent: globalThis.MouseEvent) => {
      const nextWidth = Math.min(
        maximumWidth,
        Math.max(PANEL_MIN_WIDTH_PX, startWidth + startX - moveEvent.clientX),
      );
      setPanelWidthPx(nextWidth);
    };
    const handleUp = () => {
      document.removeEventListener("mousemove", handleMove);
      document.removeEventListener("mouseup", handleUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };

    document.body.style.cursor = "ew-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", handleMove);
    document.addEventListener("mouseup", handleUp);
  };

  return (
    <SidePanelFrame
      variant="docked"
      ariaLabel="File tree inspector"
      className="h-[55%] min-h-0 w-full shrink-0 border-t border-edge bg-surface md:h-auto md:w-[var(--file-tree-inspector-width)] md:min-w-[20rem] md:max-w-[70%] md:border-l-0 md:border-t-0"
      contentClassName="gap-5 overscroll-contain px-4 sm:px-5"
      contentTestId="file-tree-inspector-scroll"
      enableScrollHotkeys
      header={
        <PanelHeader
          title={title}
          titleColor={PANEL_ACCENT}
          subtitle={subtitle}
          preserveSubtitleEnd
        />
      }
      leading={
        <SidePanelResizeHandle
          accentColor={PANEL_ACCENT}
          className="hidden rounded-none md:block"
          onMouseDown={startResize}
          title="Drag left or right to resize"
        />
      }
      onClose={onClose}
      style={panelStyle}
    >
      <FileTreeInspectorContent
        inspectedItem={inspectedItem}
        onViewInGraph={onViewInGraph}
      />
    </SidePanelFrame>
  );
};

export default FileTreeInspector;
