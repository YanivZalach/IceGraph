import { queryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { fetchFromApi } from "../../../shared/lib/api";

const catalogDataSchema = z.object({
  include_none_iceberg_catalogs: z.boolean(),
  tables: z.array(z.string()),
});

export type CatalogData = z.infer<typeof catalogDataSchema>;

export const catalogQueryKey = ["catalog", "tables"] as const;

export const catalogQueryOptions = () =>
  queryOptions({
    queryKey: catalogQueryKey,
    queryFn: ({ signal }) =>
      fetchFromApi("/tables", catalogDataSchema, { signal }),
    enabled: false,
    retry: 1,
    staleTime: 0,
  });
