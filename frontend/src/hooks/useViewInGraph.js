import { useLocation } from "@tanstack/react-router";
import {
  BASE_PATH,
  LINEAGE_NODE_ID_PARAM,
  SELECT_NODE_ID_PARAM,
} from "../appConstants";
import { TIMELINE_SEARCH_KEYS } from "../shared/lib/searchParams";

export function useViewInGraph(graphPath = "/table/graph") {
  const tabSearch = useLocation({ select: (loc) => loc.searchStr });

  const viewInGraph = (e, nodeId) => {
    e.stopPropagation();

    const url = new URL(
      `${window.location.origin}${BASE_PATH}${graphPath}${tabSearch}`,
    );
    for (const key of TIMELINE_SEARCH_KEYS) {
      url.searchParams.delete(key);
    }
    url.searchParams.delete("filetree_group_by");
    url.searchParams.delete("filetree_search");
    url.searchParams.set(SELECT_NODE_ID_PARAM, nodeId);
    url.searchParams.set(LINEAGE_NODE_ID_PARAM, nodeId);
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  return { viewInGraph };
}
