import { useHotkey } from "@tanstack/react-hotkeys";
import { useNavigate, useSearch } from "@tanstack/react-router";
import type { GraphNode } from "../model/graphModel";
import { findMatchingNodes } from "../model/nodeSearch";

const GRAPH_ROUTE = "/table/graph";

export interface GraphSearch {
  isOpen: boolean;
  query: string;
  matches: GraphNode[];
  matchIds: ReadonlySet<string>;
  changeQuery: (query: string) => void;
  close: () => void;
}

export const useGraphSearch = (
  nodes: GraphNode[],
  isVisible: (nodeId: string) => boolean,
): GraphSearch => {
  const navigate = useNavigate({ from: GRAPH_ROUTE });
  const searchQuery = useSearch({
    from: GRAPH_ROUTE,
    select: (search) => search.graph_search,
  });
  const query = searchQuery ?? "";
  const matches = findMatchingNodes(nodes, query, isVisible);

  const updateQuery = (nextQuery: string | undefined): void => {
    void navigate({
      to: ".",
      search: (previous) => ({ ...previous, graph_search: nextQuery }),
      replace: true,
    });
  };

  useHotkey(
    "Mod+F",
    () => {
      updateQuery("");
    },
    { conflictBehavior: "allow", enabled: searchQuery === undefined },
  );

  return {
    isOpen: searchQuery !== undefined,
    query,
    matches,
    matchIds: new Set(matches.map((node) => node.id)),
    changeQuery: updateQuery,
    close: () => {
      updateQuery(undefined);
    },
  };
};
