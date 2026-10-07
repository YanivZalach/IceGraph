import { useQuery } from "@tanstack/react-query";
import {
  catalogQueryOptions,
  savedCatalogQueryOptions,
  type SavedCatalog,
} from "./api/catalogQueries";

export interface CatalogTables {
  catalog: SavedCatalog | null;
  errorMessage: string | null;
  isLoading: boolean;
  isRefreshing: boolean;
  refresh: () => void;
}

const newestCatalog = (
  fetchedCatalog: SavedCatalog | null,
  savedCatalog: SavedCatalog | null,
): SavedCatalog | null => {
  if (fetchedCatalog === null || savedCatalog === null) {
    return fetchedCatalog ?? savedCatalog;
  }
  return fetchedCatalog.fetchedAt >= savedCatalog.fetchedAt
    ? fetchedCatalog
    : savedCatalog;
};

export const useCatalogTables = (): CatalogTables => {
  const savedQuery = useQuery(savedCatalogQueryOptions());
  const isSavedCatalogFresh = savedQuery.data?.isFresh ?? false;
  const catalogQuery = useQuery(
    catalogQueryOptions(
      savedQuery.isSuccess && !savedQuery.isFetching && !isSavedCatalogFresh,
    ),
  );
  const catalog = newestCatalog(
    catalogQuery.data ?? null,
    savedQuery.data?.catalog ?? null,
  );

  return {
    catalog,
    errorMessage: catalogQuery.isError ? catalogQuery.error.message : null,
    isLoading:
      catalog === null && (savedQuery.isPending || catalogQuery.isFetching),
    isRefreshing: catalogQuery.isFetching,
    refresh: () => {
      void catalogQuery.refetch();
    },
  };
};
