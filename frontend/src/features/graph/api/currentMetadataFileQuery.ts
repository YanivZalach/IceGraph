import { queryOptions } from "@tanstack/react-query";
import { fetchFromApi } from "../../../shared/lib/api";
import { graphMetadataFileSchema } from "../../table/api/graphSchemas";

const fetchCurrentMetadataFile = async (
  tableName: string,
  signal: AbortSignal,
): Promise<string> => {
  const response = await fetchFromApi(
    `/graph-metadata-file/${encodeURIComponent(tableName)}`,
    graphMetadataFileSchema,
    { cache: "no-store", signal },
  );
  return response.metadata_file;
};

export const currentMetadataFileQueryOptions = (tableName: string) =>
  queryOptions({
    queryKey: ["current-metadata-file", tableName] as const,
    queryFn: ({ signal }) => fetchCurrentMetadataFile(tableName, signal),
    enabled: tableName !== "",
    gcTime: 0,
    retry: false,
    staleTime: 0,
  });
