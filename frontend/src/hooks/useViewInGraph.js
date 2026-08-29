import { useLocation } from "@tanstack/react-router";
import { BASE_PATH, SELECT_NODE_ID_PARAM } from "../appConstants";

export function useViewInGraph() {
  const tabSearch = useLocation({ select: (loc) => loc.searchStr });

  const viewInGraph = (e, nodeId) => {
    e.stopPropagation();

    const url = new URL(
      `${window.location.origin}${BASE_PATH}/table/graph${tabSearch}`,
    );
    url.searchParams.set(SELECT_NODE_ID_PARAM, nodeId);
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  return { viewInGraph, duplicatingNodeId: null, canViewInGraph: true };
}
