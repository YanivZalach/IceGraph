import { useState, type ReactNode } from "react";
import {
  TableSpecsContext,
  resolveSpecSelection,
  type SpecSelection,
  type SpecView,
} from "../features/table/tableSpecs";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useNavigate, useRouterState, useSearch } from "@tanstack/react-router";
import {
  graphProgressQueryKey,
  graphQueryKey,
  graphQueryOptions,
  requestGraphRebuild,
} from "../features/table/api/graphQueries";

interface TableSpecsProviderProps {
  children: ReactNode;
}

export const TableSpecsProvider = ({ children }: TableSpecsProviderProps) => {
  const search = useSearch({ strict: false });
  const queryClient = useQueryClient();
  const isTablePage = useRouterState({
    select: (state) => state.location.pathname.startsWith("/table/"),
  });
  const graphRequestParameters = {
    tableName: typeof search.table === "string" ? search.table : "",
    startSnapshotId:
      typeof search.start_snapshot_id === "string"
        ? search.start_snapshot_id
        : "",
    endSnapshotId:
      typeof search.end_snapshot_id === "string" ? search.end_snapshot_id : "",
  };
  const graphQuery = useQuery({
    ...graphQueryOptions(graphRequestParameters, queryClient),
    enabled: isTablePage && graphRequestParameters.tableName !== "",
  });
  const graphProgressQuery = useQuery({
    queryKey: graphProgressQueryKey(graphRequestParameters),
    queryFn: (): Record<string, string> | null => null,
    enabled: false,
  });
  const navigate = useNavigate();
  const specKind = search.spec_kind;
  const specId = search.spec_id;
  const selection: SpecSelection | null =
    (specKind === "schema" ||
      specKind === "partition" ||
      specKind === "order") &&
    typeof specId === "string"
      ? { kind: specKind, id: specId }
      : null;
  const detailsOpen = search.specs === "open" || selection !== null;
  const specView: SpecView = search.spec_view === "diff" ? "diff" : "full";
  const selectionDetail = resolveSpecSelection(
    graphQuery.data?.metadata,
    selection,
  );
  // A shared link can name a specification the loaded metadata does not
  // contain. Report it once the graph has loaded rather than dropping it.
  const unresolvedSelection =
    selection !== null && graphQuery.data !== undefined && selectionDetail
      ? null
      : selection;

  const setSpecSearch = (
    next: {
      specs?: "open";
      spec_kind?: SpecSelection["kind"];
      spec_id?: string;
    },
    isReplacing: boolean,
    clearView = false,
  ): void => {
    void navigate({
      to: ".",
      search: (previous: Record<string, unknown>) => {
        const carried = { ...previous };
        delete carried.specs;
        delete carried.spec_kind;
        delete carried.spec_id;
        if (clearView) delete carried.spec_view;
        return { ...carried, ...next };
      },
      replace: isReplacing,
    });
  };

  const clearSpecSelection = (): void => {
    setSpecSearch({ specs: "open" }, true);
  };
  const setDetailsOpen = (isOpen: boolean): void => {
    setSpecSearch(isOpen ? { specs: "open" } : {}, !isOpen, !isOpen);
  };
  const openSpec = (next: SpecSelection): void => {
    setSpecSearch({ spec_kind: next.kind, spec_id: String(next.id) }, false);
  };
  const setSpecView = (view: SpecView): void => {
    void navigate({
      to: ".",
      search: (previous: Record<string, unknown>) => {
        const next = { ...previous };
        if (view === "diff") next.spec_view = "diff";
        else delete next.spec_view;
        return next;
      },
      replace: true,
    });
  };
  const [issuesOpen, setIssuesOpen] = useState(false);
  const errors = isTablePage ? (graphQuery.data?.errors ?? {}) : {};
  const warnings = isTablePage ? (graphQuery.data?.warnings ?? {}) : {};

  const rebuildGraph = async () => {
    if (!isTablePage || graphRequestParameters.tableName === "") return;

    requestGraphRebuild(graphRequestParameters);
    await queryClient.resetQueries({
      queryKey: graphQueryKey(graphRequestParameters),
      exact: true,
    });
  };

  return (
    <TableSpecsContext.Provider
      value={{
        detailsOpen,
        setDetailsOpen,
        selectionDetail,
        unresolvedSelection,
        clearSpecSelection,
        openSpec,
        specView,
        setSpecView,
        graphQuery,
        collectionStages: graphProgressQuery.data,
        rebuildGraph,
        errors,
        warnings,
        issuesOpen,
        setIssuesOpen,
      }}
    >
      {children}
    </TableSpecsContext.Provider>
  );
};
