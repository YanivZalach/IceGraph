import { queryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { fetchFromApi } from "../../../shared/lib/api";
import { getCachedValue, setCachedValue } from "../../../shared/lib/indexedDb";

const SAVED_CATALOG_KEY = "catalog-tables:v1";

const catalogDataSchema = z.object({
  include_none_iceberg_catalogs: z.boolean(),
  tables: z.array(z.string()),
  browser_refresh_seconds: z.number().nonnegative(),
});

const savedCatalogSchema = z.object({
  data: catalogDataSchema,
  fetchedAt: z.number(),
});

export type SavedCatalog = z.infer<typeof savedCatalogSchema>;

interface SavedCatalogState {
  catalog: SavedCatalog | null;
  isFresh: boolean;
}

const readSavedCatalog = async (): Promise<SavedCatalogState> => {
  try {
    const parsed = savedCatalogSchema.safeParse(
      await getCachedValue(SAVED_CATALOG_KEY),
    );
    if (!parsed.success) return { catalog: null, isFresh: false };
    const ageMs = Date.now() - parsed.data.fetchedAt;
    return {
      catalog: parsed.data,
      isFresh:
        ageMs >= 0 && ageMs < parsed.data.data.browser_refresh_seconds * 1000,
    };
  } catch (cacheError) {
    console.warn("Failed to read saved table list", cacheError);
    return { catalog: null, isFresh: false };
  }
};

const fetchCatalog = async (signal: AbortSignal): Promise<SavedCatalog> => {
  const catalog = {
    data: await fetchFromApi("/tables", catalogDataSchema, { signal }),
    fetchedAt: Date.now(),
  };
  void setCachedValue(SAVED_CATALOG_KEY, catalog).catch(
    (cacheError: unknown) => {
      console.warn("Failed to save table list", cacheError);
    },
  );
  return catalog;
};

export const savedCatalogQueryOptions = () =>
  queryOptions({
    queryKey: ["catalog", "saved"],
    queryFn: readSavedCatalog,
    retry: false,
  });

export const catalogQueryOptions = (isEnabled: boolean) =>
  queryOptions({
    queryKey: ["catalog", "tables"],
    queryFn: ({ signal }) => fetchCatalog(signal),
    enabled: isEnabled,
    retry: 1,
    staleTime: (query) =>
      (query.state.data?.data.browser_refresh_seconds ?? 0) * 1000,
  });
