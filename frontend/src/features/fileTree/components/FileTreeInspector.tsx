import { useEffect, useRef, useState } from "react";
import type { CSSProperties, MouseEvent } from "react";
import { PanelHeader } from "../../../components/PanelContent";
import SidePanelFrame, {
  SidePanelResizeHandle,
} from "../../../components/SidePanelFrame";
import { startHorizontalResize } from "../../../shared/lib/horizontalResize";
import type { InspectedFileTreeItem } from "../types";
import FileTreeInspectorContent from "./FileTreeInspectorContent";

interface FileTreeInspectorProps {
  duplicatingNodeId: string | null;
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
  duplicatingNodeId,
  inspectedItem,
  onClose,
  onViewInGraph,
}: FileTreeInspectorProps) => {
  const [panelWidthPx, setPanelWidthPx] = useState<number | null>(null);
  const cancelResizeRef = useRef<(() => void) | null>(null);

  useEffect(
    () => () => {
      cancelResizeRef.current?.();
    },
    [],
  );
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

  const handleResizeStart = (event: MouseEvent<HTMLDivElement>) => {
    const panel = event.currentTarget.parentElement;
    const container = panel?.parentElement;
    if (panel == null || container == null) return;

    cancelResizeRef.current = startHorizontalResize(
      event,
      {
        maximumWidthPx: container.getBoundingClientRect().width * 0.7,
        minimumWidthPx: PANEL_MIN_WIDTH_PX,
        startWidthPx: panel.getBoundingClientRect().width,
      },
      setPanelWidthPx,
    );
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
          onMouseDown={handleResizeStart}
          title="Drag left or right to resize"
        />
      }
      onClose={onClose}
      style={panelStyle}
    >
      <FileTreeInspectorContent
        duplicatingNodeId={duplicatingNodeId}
        inspectedItem={inspectedItem}
        onViewInGraph={onViewInGraph}
      />
    </SidePanelFrame>
  );
};

export default FileTreeInspector;
