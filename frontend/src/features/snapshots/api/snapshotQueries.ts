import { queryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { fetchFromApi } from "../../../shared/lib/api";

const snapshotMapSchema = z.record(
  z.string(),
  z.object({
    operation: z.string(),
    snapshot_id: z.string(),
  }),
);

export type SnapshotMap = z.infer<typeof snapshotMapSchema>;

export const snapshotMapQueryOptions = (tableName: string) =>
  queryOptions({
    queryKey: ["snapshot-map", tableName] as const,
    queryFn: ({ signal }) =>
      fetchFromApi(
        `/snapshot-map/${encodeURIComponent(tableName)}`,
        snapshotMapSchema,
        { signal },
      ),
    enabled: tableName !== "",
    retry: 1,
    staleTime: 60 * 1000,
  });
