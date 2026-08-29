import { z } from "zod";
import {
  deleteCachedValue,
  getAllCachedEntries,
  getCachedValue,
  setCachedValue,
} from "../../../shared/lib/indexedDb";
import { env } from "../../../shared/lib/env";
import { graphDataSchema, type GraphData } from "./graphSchemas";

const GRAPH_CACHE_SCHEMA_VERSION = 1;
const GRAPH_CACHE_PREFIX = `graph:v${String(GRAPH_CACHE_SCHEMA_VERSION)}:${env.appVersion}:`;
const GRAPH_CACHE_CLEANUP_KEY = "graph-cache:last-cleanup";
const GRAPH_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const GRAPH_CACHE_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;

export interface GraphRequestParameters {
  endSnapshotId: string;
  startSnapshotId: string;
  tableName: string;
}

const graphCacheEntrySchema = z.object({
  cacheKey: z.string(),
  data: graphDataSchema,
  effectiveEndSnapshotId: z.string(),
  effectiveStartSnapshotId: z.string(),
  lastAccessedAt: z.number(),
  metadataFile: z.string(),
  schemaVersion: z.literal(GRAPH_CACHE_SCHEMA_VERSION),
});

export const getGraphCacheKey = (parameters: GraphRequestParameters): string =>
  `${GRAPH_CACHE_PREFIX}${JSON.stringify([
    parameters.tableName,
    parameters.startSnapshotId,
    parameters.endSnapshotId,
  ])}`;

export const readGraphCache = async (
  parameters: GraphRequestParameters,
  metadataFile: string,
): Promise<GraphData | undefined> => {
  const cacheKey = getGraphCacheKey(parameters);
  const cachedValue = await getCachedValue(cacheKey);
  const parsedCache = graphCacheEntrySchema.safeParse(cachedValue);
  if (!parsedCache.success || parsedCache.data.metadataFile !== metadataFile) {
    return undefined;
  }

  try {
    await setCachedValue(cacheKey, {
      ...parsedCache.data,
      lastAccessedAt: Date.now(),
    });
  } catch (cacheError) {
    console.warn("Failed to update graph cache access time", cacheError);
  }
  return parsedCache.data.data;
};

export const writeGraphCache = async (
  parameters: GraphRequestParameters,
  data: GraphData,
): Promise<void> => {
  const mainMetadataNode = data.nodes.find(
    (node) => node.type === "main_metadata",
  );
  const metadataFile = mainMetadataNode?.file_path;
  if (metadataFile === undefined) {
    console.warn(
      "Graph cache was not written because main metadata is missing",
    );
    return;
  }

  const snapshots = data.nodes
    .filter((node) => node.type === "snapshot")
    .map((node) => ({
      snapshotId:
        typeof node.snapshot_id === "string" ? node.snapshot_id : undefined,
      timestamp: typeof node.timestamp === "string" ? node.timestamp : "",
    }))
    .filter(
      (snapshot): snapshot is { snapshotId: string; timestamp: string } =>
        snapshot.snapshotId !== undefined,
    )
    .sort((left, right) => left.timestamp.localeCompare(right.timestamp));
  const mainMetadataSnapshotId =
    typeof mainMetadataNode?.snapshot_id === "string"
      ? mainMetadataNode.snapshot_id
      : "";
  const effectiveStartSnapshotId =
    snapshots[0]?.snapshotId ?? parameters.startSnapshotId;
  const effectiveEndSnapshotId =
    snapshots.at(-1)?.snapshotId ||
    mainMetadataSnapshotId ||
    parameters.endSnapshotId;

  const cacheKey = getGraphCacheKey(parameters);
  await setCachedValue(cacheKey, {
    cacheKey,
    data,
    effectiveEndSnapshotId,
    effectiveStartSnapshotId,
    lastAccessedAt: Date.now(),
    metadataFile,
    schemaVersion: GRAPH_CACHE_SCHEMA_VERSION,
  });
};

export const deleteGraphCache = async (
  parameters: GraphRequestParameters,
): Promise<void> => {
  await deleteCachedValue(getGraphCacheKey(parameters));
};

export const cleanupExpiredGraphCache = async (): Promise<void> => {
  const currentTime = Date.now();
  const lastCleanupValue = await getCachedValue(GRAPH_CACHE_CLEANUP_KEY);
  const parsedLastCleanup = z.number().safeParse(lastCleanupValue);
  if (
    parsedLastCleanup.success &&
    currentTime - parsedLastCleanup.data < GRAPH_CACHE_CLEANUP_INTERVAL_MS
  ) {
    return;
  }

  const cachedEntries = await getAllCachedEntries();
  await Promise.all(
    cachedEntries.map(async ({ key, value }) => {
      const parsedCache = graphCacheEntrySchema.safeParse(value);
      if (
        parsedCache.success &&
        currentTime - parsedCache.data.lastAccessedAt > GRAPH_CACHE_MAX_AGE_MS
      ) {
        await deleteCachedValue(parsedCache.data.cacheKey);
      } else if (
        !parsedCache.success &&
        typeof key === "string" &&
        (key.startsWith("graph:") ||
          key.startsWith("graphData_") ||
          key.includes("cache_id="))
      ) {
        await deleteCachedValue(key);
      }
    }),
  );
  await setCachedValue(GRAPH_CACHE_CLEANUP_KEY, currentTime);
};
