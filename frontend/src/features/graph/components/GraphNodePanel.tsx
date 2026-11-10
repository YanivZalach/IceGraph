import { useEffect, useRef, useState } from "react";
import CopyIconButton from "../../../components/CopyIconButton";
import {
  PANEL_EMPTY_MESSAGE_CLASS,
  PANEL_STATUS_BADGE_CLASS,
  PanelHeader,
} from "../../../components/PanelContent";
import PanelIssueNotice from "../../../components/PanelIssueNotice";
import ResizableSidePanel, {
  type ResizableSidePanelLayout,
} from "../../../components/ResizableSidePanel";
import { fileTypeLabel } from "../../../graphConstants";
import {
  PANEL_WIDTH_DEFAULT_REM,
  PANEL_WIDTH_RELAXED_REM,
} from "../../../layoutConstants";
import HelpTerm from "../../../shared/components/HelpTerm";
import type { ManifestPartitionView } from "../hooks/useGraphSelection";
import { useScrollKeys } from "../hooks/useGraphKeyboard";
import { useSmoothScroll } from "../hooks/useSmoothScroll";
import type { GraphNode } from "../model/graphModel";
import GraphNodeDetails from "./GraphNodeDetails";

interface GraphNodePanelProps {
  node: GraphNode;
  isInspectMode: boolean;
  isKeyboardScrollEnabled: boolean;
  maxContainerWidth: number;
  manifestPartitionView: ManifestPartitionView | null;
  onShowManifestPartition: (
    manifest: GraphNode,
    partitionLabel: string,
  ) => void;
  onClose: () => void;
}

const CurrentMetadataNotice = ({ node }: { node: GraphNode }) =>
  node.currentMetadataStatus === "loading" ? (
    <p className={PANEL_EMPTY_MESSAGE_CLASS}>
      Loading the current metadata file…
    </p>
  ) : null;

const GraphNodePanel = ({
  node,
  isInspectMode,
  isKeyboardScrollEnabled,
  maxContainerWidth,
  manifestPartitionView,
  onShowManifestPartition,
  onClose,
}: GraphNodePanelProps) => {
  const [layout, setLayout] = useState<ResizableSidePanelLayout>({
    isFullscreen: false,
    panelWidthRem: PANEL_WIDTH_DEFAULT_REM,
  });
  const scrollRef = useRef<HTMLDivElement>(null);
  const { scrollBy, scrollToTop } = useSmoothScroll(scrollRef);
  useScrollKeys(isKeyboardScrollEnabled, scrollBy);

  useEffect(() => {
    scrollToTop();
  }, [node.id, scrollToTop]);

  return (
    <ResizableSidePanel
      ref={scrollRef}
      accentColor={node.color}
      header={
        <PanelHeader
          title={
            <span className="flex items-center gap-2">
              {fileTypeLabel(node.details.type)}
              <CopyIconButton
                key={node.id}
                text={JSON.stringify(node.details, null, 2)}
                title="Copy all node details as JSON"
                className="shrink-0"
              />
            </span>
          }
          titleColor={node.color}
        />
      }
      onClose={onClose}
      onLayoutChange={setLayout}
      maxContainerWidth={maxContainerWidth}
    >
      {isInspectMode && (
        <span className={PANEL_STATUS_BADGE_CLASS}>🔒 Locked View</span>
      )}
      {node.details.virtual_read === true && (
        <span className={PANEL_STATUS_BADGE_CLASS}>
          <HelpTerm label="Virtual read">
            Details come from Iceberg metadata. IceGraph does not read this file
            or verify that it exists.
          </HelpTerm>
        </span>
      )}
      <CurrentMetadataNotice node={node} />
      <PanelIssueNotice type="error">{node.details.errors}</PanelIssueNotice>
      <PanelIssueNotice type="warning">
        {node.details.warnings}
      </PanelIssueNotice>
      <GraphNodeDetails
        nodeId={node.id}
        details={node.details}
        isRelaxedCollapse={
          layout.isFullscreen || layout.panelWidthRem >= PANEL_WIDTH_RELAXED_REM
        }
        partitionFilter={{
          activePartitionLabel:
            manifestPartitionView?.manifest.id === node.id
              ? manifestPartitionView.partitionLabel
              : null,
          onShowPartition: (partitionLabel) => {
            onShowManifestPartition(node, partitionLabel);
          },
        }}
      />
    </ResizableSidePanel>
  );
};

export default GraphNodePanel;
