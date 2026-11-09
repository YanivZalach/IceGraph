import { useEffect, useEffectEvent, useRef, useState } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import type { GraphModel, GraphNode } from "../model/graphModel";
import { getLineage } from "../model/lineage";
import type { GraphCamera } from "./useGraphCamera";

const GRAPH_ROUTE = "/table/graph";
const INITIAL_CAMERA_DELAY_MS = 100;
const CENTER_DURATION_MS = 400;

interface GraphSelectionSearch {
  select_node_id?: string | undefined;
  lineage_node_id?: string | undefined;
  graph_mode?: "lineage" | undefined;
}

export interface GraphSelection {
  selectedNode: GraphNode | null;
  highlightedNodeIds: ReadonlySet<string> | null;
  isFullView: boolean;
  isInspectMode: boolean;
  toggleInspectMode: () => void;
  centerGraph: () => void;
  markViewChanged: () => void;
  selectNode: (node: GraphNode) => void;
  navigateToNode: (node: GraphNode) => void;
  closePanel: () => void;
  resetView: () => void;
}

const findNode = (
  model: GraphModel,
  nodeId: string | undefined,
): GraphNode | null =>
  nodeId === undefined ? null : (model.nodesById.get(nodeId) ?? null);

export const useGraphSelection = (
  model: GraphModel,
  camera: GraphCamera,
): GraphSelection => {
  const navigate = useNavigate({ from: GRAPH_ROUTE });
  const search = useSearch({ from: GRAPH_ROUTE });
  const [isViewMoved, setIsViewMoved] = useState(false);
  const hasPlacedCameraRef = useRef(false);

  const selectedNode = findNode(model, search.select_node_id);
  const lineageRoot = findNode(model, search.lineage_node_id);
  const isInspectMode = search.graph_mode !== "lineage";
  const highlightedNodeIds =
    lineageRoot === null ? null : getLineage(model.lineage, lineageRoot.id);
  const isFullView = lineageRoot === null && !isViewMoved;
  const hasUnknownNodeIds =
    (search.select_node_id !== undefined && selectedNode === null) ||
    (search.lineage_node_id !== undefined && lineageRoot === null);

  const updateSearch = (
    changes: GraphSelectionSearch,
    replace: boolean,
  ): void => {
    void navigate({
      to: ".",
      search: (previous) => ({ ...previous, ...changes }),
      replace,
    });
  };

  const placeCamera = useEffectEvent((): void => {
    const isFirstPlacement = !hasPlacedCameraRef.current;
    hasPlacedCameraRef.current = true;
    window.setTimeout(
      () => {
        if (lineageRoot === null || highlightedNodeIds === null) {
          setIsViewMoved(false);
          camera.resetZoom();
        } else if (isFirstPlacement) {
          camera.fitNodes(highlightedNodeIds);
        } else {
          camera.centerOn(lineageRoot, CENTER_DURATION_MS);
        }
      },
      isFirstPlacement ? INITIAL_CAMERA_DELAY_MS : 0,
    );
  });

  const removeUnknownNodeIds = useEffectEvent((): void => {
    updateSearch(
      {
        select_node_id: selectedNode?.id,
        lineage_node_id: lineageRoot?.id,
      },
      true,
    );
  });

  useEffect(() => {
    placeCamera();
  }, [lineageRoot?.id]);

  useEffect(() => {
    if (hasUnknownNodeIds) removeUnknownNodeIds();
  }, [hasUnknownNodeIds]);

  return {
    selectedNode,
    highlightedNodeIds,
    isFullView,
    isInspectMode,
    toggleInspectMode: () => {
      updateSearch({ graph_mode: isInspectMode ? "lineage" : undefined }, true);
    },
    centerGraph: () => {
      setIsViewMoved(false);
      camera.resetZoom();
    },
    markViewChanged: () => {
      if (isFullView) setIsViewMoved(true);
    },
    selectNode: (node) => {
      updateSearch(
        isInspectMode
          ? { select_node_id: node.id }
          : { select_node_id: node.id, lineage_node_id: node.id },
        false,
      );
    },
    navigateToNode: (node) => {
      updateSearch(
        { select_node_id: node.id, lineage_node_id: node.id },
        false,
      );
    },
    closePanel: () => {
      updateSearch({ select_node_id: undefined }, true);
    },
    resetView: () => {
      updateSearch(
        { select_node_id: undefined, lineage_node_id: undefined },
        true,
      );
      setIsViewMoved(false);
      if (lineageRoot === null) camera.resetZoom();
    },
  };
};
