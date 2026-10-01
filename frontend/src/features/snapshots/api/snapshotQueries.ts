import { infiniteQueryOptions } from "@tanstack/react-query";
import { z } from "zod";
import { fetchFromApi } from "../../../shared/lib/api";

const snapshotMapSchema = z.record(
  z.string(),
  z.object({
    operation: z.string(),
    snapshot_id: z.string(),
  }),
);

const snapshotMapPageSchema = z.object({
  snapshots: snapshotMapSchema,
  next_before_snapshot_id: z.string().nullable(),
});

export type SnapshotMap = z.infer<typeof snapshotMapSchema>;

export const snapshotMapQueryOptions = (tableName: string) =>
  infiniteQueryOptions({
    queryKey: ["snapshot-map", tableName] as const,
    queryFn: ({ pageParam, signal }) =>
      fetchFromApi(
        `/snapshot-map/${encodeURIComponent(tableName)}${
          pageParam === ""
            ? ""
            : `?${new URLSearchParams({ before_snapshot_id: pageParam }).toString()}`
        }`,
        snapshotMapPageSchema,
        { signal },
      ),
    initialPageParam: "",
    getNextPageParam: (lastPage) => lastPage.next_before_snapshot_id,
    enabled: tableName !== "",
    retry: 1,
    staleTime: 60 * 1000,
  });
