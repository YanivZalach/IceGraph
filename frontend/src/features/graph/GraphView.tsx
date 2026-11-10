import { useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSearch } from "@tanstack/react-router";
import ForceGraph2D from "react-force-graph-2d";
import { PANEL_GUTTER_REM, remToPx } from "../../layoutConstants";
import { currentMetadataFileQueryOptions } from "./api/currentMetadataFileQuery";
import { readFrameBounds, UNBOUNDED_FRAME } from "./canvas/frameBounds";
import { paintLinks } from "./canvas/paintLink";
import { paintNode, paintNodePointerArea } from "./canvas/paintNode";
import GraphNeighborPopup from "./components/GraphNeighborPopup";
import GraphNodePanel from "./components/GraphNodePanel";
import GraphPartitionBanner from "./components/GraphPartitionBanner";
import GraphSearchBox from "./components/GraphSearchBox";
import GraphToolbar from "./components/GraphToolbar";
import { useCanvasGesture } from "./hooks/useCanvasGesture";
import { useGraphCamera, type GraphMethods } from "./hooks/useGraphCamera";
import { useGraphKeyboard } from "./hooks/useGraphKeyboard";
import { useGraphSearch } from "./hooks/useGraphSearch";
import { useGraphSelection } from "./hooks/useGraphSelection";
import { useGraphViewport } from "./hooks/useGraphViewport";
import {
  buildGraphModel,
  type FileGraph,
  type GraphLink,
  type GraphNode,
} from "./model/graphModel";
import { getNeighborIds, type NeighborDirection } from "./model/lineage";
import {
  buildNeighborPopup,
  type NeighborPopupState,
} from "./model/neighborPopup";

interface GraphViewProps {
  fileGraph: FileGraph;
}

const NO_FORCE_GRAPH_LINKS: GraphLink[] = [];
const SEARCH_CENTER_DURATION_MS = 400;

const GraphView = ({ fileGraph }: GraphViewProps) => {
  const tableName = useSearch({ from: "/table/graph" }).table ?? "";
  const currentMetadataQuery = useQuery(
    currentMetadataFileQueryOptions(tableName),
  );
  const model = buildGraphModel(
    fileGraph,
    currentMetadataQuery.data,
    currentMetadataQuery.error,
  );
  const forceGraphData = { nodes: model.nodes, links: NO_FORCE_GRAPH_LINKS };

  const graphRef = useRef<GraphMethods>(undefined);
  const frameRef = useRef(UNBOUNDED_FRAME);
  const viewport = useGraphViewport();
  const camera = useGraphCamera(graphRef, model.nodes, viewport);
  const selection = useGraphSelection(model, camera);
  const [neighborPopup, setNeighborPopup] = useState<NeighborPopupState | null>(
    null,
  );

  const partialManifestId = selection.manifestPartitionView?.manifest.id;

  const isHighlighted = (nodeId: string): boolean =>
    selection.highlightedNodeIds === null ||
    selection.highlightedNodeIds.has(nodeId);

  const search = useGraphSearch(model.nodes, isHighlighted);

  const handleSearchPick = (node: GraphNode): void => {
    setNeighborPopup(null);
    selection.selectNode(node);
    camera.centerOn(node, SEARCH_CENTER_DURATION_MS);
  };

  const handleNavigateToNode = (node: GraphNode): void => {
    setNeighborPopup(null);
    const currentNode = model.nodesById.get(node.id);
    if (currentNode !== undefined) selection.navigateToNode(currentNode);
  };

  const handleNavigateNeighbors = (direction: NeighborDirection): void => {
    const selectedNode = selection.selectedNode;
    if (selectedNode === null) return;
    const neighbors = getNeighborIds(model.lineage, selectedNode.id, direction)
      .map((neighborId) => model.nodesById.get(neighborId))
      .filter((node) => node !== undefined);
    const [onlyNeighbor] = neighbors;
    if (onlyNeighbor === undefined) return;
    if (neighbors.length === 1) handleNavigateToNode(onlyNeighbor);
    else setNeighborPopup(buildNeighborPopup(neighbors, direction));
  };

  const handleResetView = (): void => {
    setNeighborPopup(null);
    selection.resetView();
  };

  const canvasGesture = useCanvasGesture(selection.markViewChanged);

  const handleSelectMainMetadata = (): void => {
    if (fileGraph.mainMetadata !== null) {
      handleNavigateToNode(fileGraph.mainMetadata);
    }
  };

  useGraphKeyboard({
    isNeighborPopupOpen: neighborPopup !== null,
    hasSelectedNode: selection.selectedNode !== null,
    isInspectMode: selection.isInspectMode,
    onToggleInspectMode: selection.toggleInspectMode,
    onCenterGraph: selection.centerGraph,
    onResetView: handleResetView,
    onClosePanel: selection.closePanel,
    onSelectMainMetadata: handleSelectMainMetadata,
    onNavigateNeighbors: handleNavigateNeighbors,
  });

  return (
    <div
      className="relative h-graph w-full overflow-hidden bg-graph-grid"
      onWheelCapture={canvasGesture.onWheelCapture}
      onPointerDownCapture={canvasGesture.onPointerDownCapture}
      onPointerMoveCapture={canvasGesture.onPointerMoveCapture}
    >
      <ForceGraph2D<GraphNode, GraphLink>
        ref={graphRef}
        width={viewport.width}
        height={viewport.height}
        graphData={forceGraphData}
        backgroundColor="#00000000"
        nodeLabel={() => ""}
        onRenderFramePre={(context, globalScale) => {
          frameRef.current = readFrameBounds(context, globalScale);
          paintLinks(model.links, model.nodesById, isHighlighted, {
            context,
            metrics: viewport.nodeMetrics,
            bounds: frameRef.current,
          });
        }}
        nodeCanvasObject={(node, context) => {
          paintNode(
            node,
            context,
            viewport.nodeMetrics,
            frameRef.current,
            search.matchIds.has(node.id),
            node.id === partialManifestId,
          );
        }}
        nodePointerAreaPaint={(node, color, context) => {
          paintNodePointerArea(node, color, context, viewport.nodeMetrics);
        }}
        nodeVisibility={(node) => isHighlighted(node.id)}
        onNodeClick={selection.selectNode}
        onNodeDrag={selection.markViewChanged}
        onNodeDragEnd={selection.markViewChanged}
        warmupTicks={1}
        cooldownTicks={0}
        d3AlphaDecay={1}
      />

      <GraphToolbar
        isFullView={selection.isFullView}
        isInspectMode={selection.isInspectMode}
        onResetView={handleResetView}
        onToggleInspectMode={selection.toggleInspectMode}
        onCenterGraph={selection.centerGraph}
        search={
          search.isOpen ? (
            <GraphSearchBox
              search={search}
              selectedNodeId={selection.selectedNode?.id ?? null}
              onPick={handleSearchPick}
            />
          ) : null
        }
        notice={
          selection.manifestPartitionView === null ? null : (
            <GraphPartitionBanner
              view={selection.manifestPartitionView}
              onShowFullManifest={selection.showFullManifest}
            />
          )
        }
      />

      {neighborPopup !== null && (
        <GraphNeighborPopup
          popup={neighborPopup}
          onSelect={handleNavigateToNode}
          onClose={() => {
            setNeighborPopup(null);
          }}
        />
      )}

      {selection.selectedNode !== null && (
        <GraphNodePanel
          node={selection.selectedNode}
          isInspectMode={selection.isInspectMode}
          isKeyboardScrollEnabled={neighborPopup === null}
          maxContainerWidth={viewport.width - remToPx(PANEL_GUTTER_REM)}
          manifestPartitionView={selection.manifestPartitionView}
          onShowManifestPartition={selection.showManifestPartition}
          onClose={selection.closePanel}
        />
      )}
    </div>
  );
};

export default GraphView;
