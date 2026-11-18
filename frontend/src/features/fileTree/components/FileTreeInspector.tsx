import type { MouseEvent } from "react";
import { fileTypeLabel } from "../../../graphConstants.js";
import { PanelHeader } from "../../../components/PanelContent";
import SidePanelFrame from "../../../components/SidePanelFrame";
import type { InspectedFileTreeItem } from "../types";
import FileTreeInspectorContent from "./FileTreeInspectorContent";

interface FileTreeInspectorProps {
  inspectedItem: InspectedFileTreeItem;
  onClose: () => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

const PANEL_ACCENT = "#2e86c1";

const FileTreeInspector = ({
  inspectedItem,
  onClose,
  onViewInGraph,
}: FileTreeInspectorProps) => {
  const title =
    inspectedItem.kind === "file"
      ? fileTypeLabel(inspectedItem.file.type)
      : inspectedItem.kind === "partition-path"
        ? "Partition path statistics"
        : "Partition statistics";
  const subtitle =
    inspectedItem.kind === "file"
      ? inspectedItem.file.id
      : inspectedItem.kind === "partition-path"
        ? inspectedItem.partitionPathNode.path
        : inspectedItem.partition.name;

  return (
    <SidePanelFrame
      variant="docked"
      ariaLabel="File tree inspector"
      className="h-full min-h-0 w-full"
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
      onClose={onClose}
    >
      <FileTreeInspectorContent
        inspectedItem={inspectedItem}
        onViewInGraph={onViewInGraph}
      />
    </SidePanelFrame>
  );
};

export default FileTreeInspector;
