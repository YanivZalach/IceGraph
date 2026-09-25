import { useEffect, useState, type ReactNode } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import {
  useNavigate,
  useRouter,
  useRouterState,
  useSearch,
} from "@tanstack/react-router";
import {
  TableSpecsContext,
  hasPreviousSpec,
  isSpecsOverlayState,
  parseSpecSelection,
  resolveSpecSelection,
  type SpecSelection,
  type SpecSearch,
  type SpecView,
} from "./tableSpecs";
import {
  graphProgressQueryOptions,
  graphQueryKey,
  graphQueryOptions,
  requestGraphRebuild,
} from "../table/api/graphQueries";
import { tableMetadataQueryOptions } from "../table/api/tableMetadataQueries";

export const TableSpecsProvider = ({ children }: { children: ReactNode }) => {
  const search = useSearch({ strict: false });
  const navigate = useNavigate();
  const router = useRouter();
  const queryClient = useQueryClient();
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const isTablePage = pathname.startsWith("/table/");
  const isSnapshotSelectionPage = pathname === "/snapshots-selection";
  const isSpecsHistoryEntry = useRouterState({
    select: (state) => isSpecsOverlayState(state.location.state),
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
  const tableMetadataQuery = useQuery({
    ...tableMetadataQueryOptions(graphRequestParameters.tableName),
    enabled: isSnapshotSelectionPage && graphRequestParameters.tableName !== "",
  });
  const specsMetadata = isTablePage
    ? graphQuery.data?.metadata
    : isSnapshotSelectionPage
      ? tableMetadataQuery.data
      : undefined;
  const graphProgressQuery = useQuery(
    graphProgressQueryOptions(graphRequestParameters),
  );
  const specKind = search.spec_kind;
  const specId = search.spec_id;
  const selection = parseSpecSelection(specKind, specId);
  const hasIncompleteSpecSelection =
    (specKind !== undefined || specId !== undefined) && selection === null;
  const detailsOpen = search.specs === "open" || selection !== null;
  const specView: SpecView = search.spec_view === "diff" ? "diff" : "full";
  const selectionDetail = resolveSpecSelection(specsMetadata, selection);
  const unresolvedSelection =
    specsMetadata !== undefined && selectionDetail === null ? selection : null;

  const navigateSpecs = (
    next: SpecSearch,
    options: {
      clearView?: boolean;
      markEntry?: boolean;
      replace: boolean;
    },
  ): void => {
    void navigate({
      to: ".",
      search: (previous: Record<string, unknown>) => {
        const carried = { ...previous };
        delete carried.specs;
        delete carried.spec_kind;
        delete carried.spec_id;
        if (options.clearView) delete carried.spec_view;
        return { ...carried, ...next };
      },
      state: (previous) => ({
        ...previous,
        specsOverlay: options.markEntry || isSpecsOverlayState(previous),
      }),
      replace: options.replace,
    });
  };

  const closeSpecs = (): void => {
    if (isSpecsHistoryEntry) {
      router.history.back();
      return;
    }
    navigateSpecs({}, { clearView: true, replace: true });
  };

  const setDetailsOpen = (isOpen: boolean): void => {
    if (!isOpen) {
      closeSpecs();
      return;
    }
    navigateSpecs({ specs: "open" }, { markEntry: true, replace: detailsOpen });
  };

  const clearSpecSelection = (): void => {
    navigateSpecs({ specs: "open" }, { replace: true });
  };

  const openSpec = (next: SpecSelection): void => {
    const canShowDiff = hasPreviousSpec(specsMetadata, next);
    navigateSpecs(
      { spec_kind: next.kind, spec_id: String(next.id) },
      {
        clearView: !canShowDiff,
        markEntry: !detailsOpen,
        replace: detailsOpen,
      },
    );
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

  const shouldClearDiff =
    specView === "diff" &&
    selection !== null &&
    selectionDetail !== null &&
    !hasPreviousSpec(specsMetadata, selection);
  useEffect(() => {
    if (!shouldClearDiff && !hasIncompleteSpecSelection) return;
    void navigate({
      to: ".",
      search: (previous: Record<string, unknown>) => {
        const next = { ...previous };
        if (shouldClearDiff) delete next.spec_view;
        if (hasIncompleteSpecSelection) {
          delete next.spec_kind;
          delete next.spec_id;
        }
        return next;
      },
      replace: true,
    });
  }, [hasIncompleteSpecSelection, navigate, shouldClearDiff]);

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
        specsMetadata,
        graphQuery,
        collectionStages: graphProgressQuery.data,
        rebuildGraph,
        errors,
        warnings,
        issuesOpen: isTablePage && issuesOpen,
        setIssuesOpen,
      }}
    >
      {children}
    </TableSpecsContext.Provider>
  );
};
