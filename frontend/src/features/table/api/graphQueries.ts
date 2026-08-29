import { queryOptions, type QueryClient } from "@tanstack/react-query";
import { fetchFromApi } from "../../../shared/lib/api";
import {
  clearHardRefreshRequest,
  shouldBypassPersistentGraphCache,
} from "../../../shared/lib/hardRefresh";
import {
  cleanupExpiredGraphCache,
  readGraphCache,
  writeGraphCache,
  type GraphRequestParameters,
} from "./graphCache";
import {
  graphJobPollResponseSchema,
  graphJobSubmissionSchema,
  graphMetadataFileSchema,
  type GraphData,
} from "./graphSchemas";

const GRAPH_POLL_INTERVAL_MS = 700;
const GRAPH_CACHE_READ_TIMEOUT_MS = 5000;
const requestedGraphRebuilds = new Set<string>();

export const graphQueryKey = (parameters: GraphRequestParameters) =>
  [
    "graph",
    parameters.tableName,
    parameters.startSnapshotId,
    parameters.endSnapshotId,
  ] as const;

export const graphProgressQueryKey = (parameters: GraphRequestParameters) =>
  [...graphQueryKey(parameters), "progress"] as const;

const getGraphRequestKey = (parameters: GraphRequestParameters): string =>
  JSON.stringify(graphQueryKey(parameters));

export const requestGraphRebuild = (
  parameters: GraphRequestParameters,
): void => {
  requestedGraphRebuilds.add(getGraphRequestKey(parameters));
};

const shouldRebuildGraph = (parameters: GraphRequestParameters): boolean =>
  requestedGraphRebuilds.has(getGraphRequestKey(parameters));

const clearGraphRebuildRequest = (parameters: GraphRequestParameters): void => {
  requestedGraphRebuilds.delete(getGraphRequestKey(parameters));
};

const cleanupGraphCacheInBackground = (): void => {
  void cleanupExpiredGraphCache().catch((cacheError: unknown) => {
    console.warn("Failed to clean expired graph caches", cacheError);
  });
};

const waitForNextPoll = (signal: AbortSignal): Promise<void> => {
  return new Promise((resolve, reject) => {
    const handleAbort = () => {
      window.clearTimeout(timeoutId);
      reject(new DOMException("Graph request aborted", "AbortError"));
    };
    const timeoutId = window.setTimeout(() => {
      signal.removeEventListener("abort", handleAbort);
      resolve();
    }, GRAPH_POLL_INTERVAL_MS);
    signal.addEventListener("abort", handleAbort, { once: true });
  });
};

const readGraphCacheWithTimeout = async (
  parameters: GraphRequestParameters,
  metadataFile: string,
): Promise<GraphData | undefined> => {
  let timeoutId: number | undefined;

  try {
    return await Promise.race([
      readGraphCache(parameters, metadataFile),
      new Promise<never>((_, reject) => {
        timeoutId = window.setTimeout(() => {
          reject(new Error("Graph cache read timed out"));
        }, GRAPH_CACHE_READ_TIMEOUT_MS);
      }),
    ]);
  } finally {
    window.clearTimeout(timeoutId);
  }
};

const fetchGraphMetadataFile = async (
  parameters: GraphRequestParameters,
  signal: AbortSignal,
): Promise<string> => {
  const searchParameters = new URLSearchParams();
  if (parameters.endSnapshotId !== "") {
    searchParameters.set("end_snapshot_id", parameters.endSnapshotId);
  }
  const queryString = searchParameters.toString();
  const response = await fetchFromApi(
    `/graph-metadata-file/${encodeURIComponent(parameters.tableName)}${queryString === "" ? "" : `?${queryString}`}`,
    graphMetadataFileSchema,
    { signal },
  );
  return response.metadata_file;
};

const buildGraph = async (
  parameters: GraphRequestParameters,
  queryClient: QueryClient,
  signal: AbortSignal,
): Promise<GraphData> => {
  const formBody = new URLSearchParams({ table_name: parameters.tableName });
  if (parameters.startSnapshotId !== "") {
    formBody.set("start_snapshot_id", parameters.startSnapshotId);
  }
  if (parameters.endSnapshotId !== "") {
    formBody.set("end_snapshot_id", parameters.endSnapshotId);
  }

  const submittedJob = await fetchFromApi(
    "/graph-data",
    graphJobSubmissionSchema,
    { formBody, method: "POST", signal },
  );

  while (!signal.aborted) {
    const jobResult = await fetchFromApi(
      `/graph-data/${encodeURIComponent(submittedJob.key)}`,
      graphJobPollResponseSchema,
      {
        headers: {
          "X-IceGraph-Job-Token": submittedJob["X-IceGraph-Job-Token"],
        },
        signal,
      },
    );

    if ("nodes" in jobResult) {
      queryClient.setQueryData(graphProgressQueryKey(parameters), null);
      void writeGraphCache(parameters, jobResult).catch(
        (cacheError: unknown) => {
          console.warn("Failed to persist graph cache", cacheError);
        },
      );
      clearGraphRebuildRequest(parameters);
      clearHardRefreshRequest();
      return jobResult;
    }

    queryClient.setQueryData(
      graphProgressQueryKey(parameters),
      jobResult.stages ?? null,
    );
    await waitForNextPoll(signal);
  }

  throw new DOMException("Graph request aborted", "AbortError");
};

const loadGraph = async (
  parameters: GraphRequestParameters,
  queryClient: QueryClient,
  signal: AbortSignal,
): Promise<GraphData> => {
  queryClient.setQueryData(graphProgressQueryKey(parameters), null);

  if (!shouldRebuildGraph(parameters) && !shouldBypassPersistentGraphCache()) {
    try {
      const metadataFile = await fetchGraphMetadataFile(parameters, signal);
      const cachedGraph = await readGraphCacheWithTimeout(
        parameters,
        metadataFile,
      );
      if (cachedGraph !== undefined) {
        cleanupGraphCacheInBackground();
        return cachedGraph;
      }
    } catch (cacheError) {
      if (signal.aborted) {
        throw new DOMException("Graph request aborted", "AbortError");
      }
      console.warn(
        "Graph cache validation failed, rebuilding graph",
        cacheError,
      );
    }
  }

  cleanupGraphCacheInBackground();
  return buildGraph(parameters, queryClient, signal);
};

export const graphQueryOptions = (
  parameters: GraphRequestParameters,
  queryClient: QueryClient,
) =>
  queryOptions({
    queryKey: graphQueryKey(parameters),
    queryFn: ({ signal }) => loadGraph(parameters, queryClient, signal),
    enabled: parameters.tableName !== "",
    gcTime: 24 * 60 * 60 * 1000,
    retry: false,
    staleTime: Number.POSITIVE_INFINITY,
  });
