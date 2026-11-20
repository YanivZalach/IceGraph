import { queryOptions, type QueryClient } from "@tanstack/react-query";
import { fetchFromApi } from "../../../shared/lib/api";
import { graphJobSubmissionSchema } from "../../table/api/graphSchemas";
import type { GraphStages } from "../../table/api/graphSchemas";
import type { PartitionFilterValue } from "../partitionFilters";
import {
  partitionFilesPollResponseSchema,
  type PartitionFiles,
} from "./partitionFilesSchemas";

const POLL_INTERVAL_MS = 700;

export interface PartitionFilesRequest {
  tableName: string;
  snapshotId: string | null;
  filters: PartitionFilterValue[];
}

const partitionFilesQueryKey = (request: PartitionFilesRequest) =>
  [
    "partition-files",
    request.tableName,
    request.snapshotId,
    [...request.filters]
      .sort((first, second) => first.name.localeCompare(second.name))
      .map(({ name, value }) => [name, value]),
  ] as const;

const partitionFilesProgressQueryKey = (request: PartitionFilesRequest) =>
  [...partitionFilesQueryKey(request), "progress"] as const;

export const partitionFilesProgressQueryOptions = (
  request: PartitionFilesRequest,
) =>
  queryOptions({
    queryKey: partitionFilesProgressQueryKey(request),
    queryFn: (): GraphStages | null => null,
    enabled: false,
  });

const waitForNextPoll = (signal: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    const handleAbort = () => {
      window.clearTimeout(timeoutId);
      reject(new DOMException("Partition files request aborted", "AbortError"));
    };
    const timeoutId = window.setTimeout(() => {
      signal.removeEventListener("abort", handleAbort);
      resolve();
    }, POLL_INTERVAL_MS);
    signal.addEventListener("abort", handleAbort, { once: true });
  });

const loadPartitionFiles = async (
  request: PartitionFilesRequest,
  queryClient: QueryClient,
  signal: AbortSignal,
): Promise<PartitionFiles> => {
  const progressKey = partitionFilesProgressQueryKey(request);
  queryClient.setQueryData(progressKey, null);
  const formBody = new URLSearchParams({
    table_name: request.tableName,
    filters: JSON.stringify(
      Object.fromEntries(
        request.filters.map(({ name, value }) => [name, value]),
      ),
    ),
  });
  if (request.snapshotId !== null)
    formBody.set("snapshot_id", request.snapshotId);
  const submittedJob = await fetchFromApi(
    "/partition-files",
    graphJobSubmissionSchema,
    { formBody, method: "POST", signal },
  );

  while (!signal.aborted) {
    const jobResult = await fetchFromApi(
      `/partition-files/${encodeURIComponent(submittedJob.key)}`,
      partitionFilesPollResponseSchema,
      {
        headers: {
          "X-IceGraph-Job-Token": submittedJob["X-IceGraph-Job-Token"],
        },
        signal,
      },
    );
    if ("nodes" in jobResult) {
      queryClient.setQueryData(progressKey, null);
      return jobResult;
    }
    queryClient.setQueryData(progressKey, jobResult.stages ?? null);
    await waitForNextPoll(signal);
  }

  throw new DOMException("Partition files request aborted", "AbortError");
};

export const partitionFilesQueryOptions = (
  request: PartitionFilesRequest,
  queryClient: QueryClient,
) =>
  queryOptions({
    queryKey: partitionFilesQueryKey(request),
    queryFn: ({ signal }) => loadPartitionFiles(request, queryClient, signal),
    enabled: request.tableName !== "" && request.filters.length > 0,
    gcTime: 60 * 60 * 1000,
    retry: false,
    staleTime: Number.POSITIVE_INFINITY,
  });
