import { useHotkey } from "@tanstack/react-hotkeys";
import { useNavigate, useSearch } from "@tanstack/react-router";
import type { GraphNode } from "../model/graphModel";
import { findMatchingNodes } from "../model/nodeSearch";

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
  const navigate = useNavigate();
  const searchQuery = useSearch({
    strict: false,
    select: (search) =>
      typeof search.graph_search === "string" ? search.graph_search : undefined,
  });
  const query = searchQuery ?? "";
  const matches = findMatchingNodes(nodes, query, isVisible);

  const updateQuery = (nextQuery: string | undefined): void => {
    void navigate({
      to: ".",
      search: (previous: Record<string, unknown>) => ({
        ...previous,
        graph_search: nextQuery,
      }),
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
