import { createContext, useContext, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useRouterState, useSearch } from "@tanstack/react-router";
import {
  graphProgressQueryKey,
  graphQueryKey,
  graphQueryOptions,
  requestGraphRebuild,
} from "../features/table/api/graphQueries";

const TableSpecsContext = createContext();

export function TableSpecsProvider({ children }) {
  const search = useSearch({ strict: false });
  const queryClient = useQueryClient();
  const isTablePage = useRouterState({
    select: (state) => state.location.pathname.startsWith("/table/"),
  });
  const graphRequestParameters = {
    tableName: search.table ?? "",
    startSnapshotId: search.start_snapshot_id ?? "",
    endSnapshotId: search.end_snapshot_id ?? "",
  };
  const graphQuery = useQuery({
    ...graphQueryOptions(graphRequestParameters, queryClient),
    enabled: isTablePage && graphRequestParameters.tableName !== "",
  });
  const graphProgressQuery = useQuery({
    queryKey: graphProgressQueryKey(graphRequestParameters),
    queryFn: async () => null,
    enabled: false,
  });
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [selectionDetail, setSelectionDetail] = useState(null);
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
        setSelectionDetail,
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
}

export function useTableSpecs() {
  return useContext(TableSpecsContext);
}
