import { queryOptions } from "@tanstack/react-query";
import { fetchFromApi } from "../../../shared/lib/api";
import { tableMetadataSchema } from "./metadataSchemas";

export const tableMetadataQueryOptions = (tableName: string) =>
  queryOptions({
    queryKey: ["table-metadata", tableName] as const,
    queryFn: ({ signal }) =>
      fetchFromApi(
        `/table-metadata/${encodeURIComponent(tableName)}`,
        tableMetadataSchema,
        { signal },
      ),
    enabled: tableName !== "",
    retry: 1,
    staleTime: 60 * 1000,
  });
