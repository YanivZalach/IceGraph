import { useLocation } from "@tanstack/react-router";
import {
  BASE_PATH,
  LINEAGE_NODE_ID_PARAM,
  SELECT_NODE_ID_PARAM,
} from "../appConstants";
import { TIMELINE_SEARCH_KEYS } from "../shared/lib/searchParams";

export function useViewInGraph() {
  const tabSearch = useLocation({ select: (loc) => loc.searchStr });

  const viewInGraph = (e, nodeId) => {
    e.stopPropagation();

    const url = new URL(
      `${window.location.origin}${BASE_PATH}/table/graph${tabSearch}`,
    );
    for (const key of TIMELINE_SEARCH_KEYS) {
      url.searchParams.delete(key);
    }
    url.searchParams.set(SELECT_NODE_ID_PARAM, nodeId);
    url.searchParams.set(LINEAGE_NODE_ID_PARAM, nodeId);
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  return { viewInGraph };
}
