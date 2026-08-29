import { z } from "zod";
import {
  deleteCachedValue,
  deleteCachedValues,
  getAllCachedKeys,
  getCachedEntriesByPrefix,
  getCachedValue,
  setCachedValue,
  setCachedValues,
} from "../../../shared/lib/indexedDb";
import { env } from "../../../shared/lib/env";
import { graphDataSchema, type GraphData } from "./graphSchemas";

const GRAPH_CACHE_SCHEMA_VERSION = 1;
const GRAPH_CACHE_PREFIX = `graph:v${String(GRAPH_CACHE_SCHEMA_VERSION)}:${env.appVersion}:`;
const GRAPH_CACHE_INDEX_PREFIX = "graph-cache-index:";
const GRAPH_CACHE_CLEANUP_KEY = "graph-cache:last-cleanup";
const GRAPH_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000;
const GRAPH_CACHE_CLEANUP_INTERVAL_MS = 60 * 60 * 1000;
const GRAPH_CACHE_MAX_ENTRIES = 20;

export interface GraphRequestParameters {
  endSnapshotId: string;
  startSnapshotId: string;
  tableName: string;
}

const graphCacheEntrySchema = z.object({
  data: graphDataSchema,
  metadataFile: z.string(),
  schemaVersion: z.literal(GRAPH_CACHE_SCHEMA_VERSION),
});

const graphCacheIndexEntrySchema = z.object({
  cacheKey: z.string(),
  lastAccessedAt: z.number(),
});

export const getGraphCacheKey = (parameters: GraphRequestParameters): string =>
  `${GRAPH_CACHE_PREFIX}${JSON.stringify([
    parameters.tableName,
    parameters.startSnapshotId,
    parameters.endSnapshotId,
  ])}`;

const getGraphCacheIndexKey = (cacheKey: string): string =>
  `${GRAPH_CACHE_INDEX_PREFIX}${cacheKey}`;

const writeGraphCacheIndex = async (cacheKey: string): Promise<void> => {
  await setCachedValue(getGraphCacheIndexKey(cacheKey), {
    cacheKey,
    lastAccessedAt: Date.now(),
  });
};

const writeGraphCacheRecord = async (
  cacheKey: string,
  cacheEntry: z.infer<typeof graphCacheEntrySchema>,
): Promise<void> => {
  await setCachedValues([
    { key: cacheKey, value: cacheEntry },
    {
      key: getGraphCacheIndexKey(cacheKey),
      value: { cacheKey, lastAccessedAt: Date.now() },
    },
  ]);
};

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

  void writeGraphCacheIndex(cacheKey).catch((cacheError: unknown) => {
    console.warn("Failed to update graph cache access time", cacheError);
  });
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

  const cacheKey = getGraphCacheKey(parameters);
  const cacheEntry: z.infer<typeof graphCacheEntrySchema> = {
    data,
    metadataFile,
    schemaVersion: GRAPH_CACHE_SCHEMA_VERSION,
  };

  await pruneGraphCacheEntries(Date.now(), GRAPH_CACHE_MAX_ENTRIES - 1);
  try {
    await writeGraphCacheRecord(cacheKey, cacheEntry);
  } catch (cacheError) {
    if (!isQuotaExceededError(cacheError)) throw cacheError;

    await evictOldestGraphCacheEntry(cacheKey);
    await writeGraphCacheRecord(cacheKey, cacheEntry);
  }
};

const isQuotaExceededError = (error: unknown): boolean =>
  typeof error === "object" &&
  error !== null &&
  "name" in error &&
  error.name === "QuotaExceededError";

const getGraphCacheRecords = async () => {
  const cachedEntries = await getCachedEntriesByPrefix(
    GRAPH_CACHE_INDEX_PREFIX,
  );
  return cachedEntries.flatMap(({ key, value }) => {
    if (typeof key !== "string") return [];

    const parsedCache = graphCacheIndexEntrySchema.safeParse(value);
    return parsedCache.success
      ? [
          {
            cacheKey: parsedCache.data.cacheKey,
            indexKey: key,
            lastAccessedAt: parsedCache.data.lastAccessedAt,
          },
        ]
      : [];
  });
};

const deleteGraphCacheRecord = async (
  cacheKey: string,
  indexKey: string,
): Promise<void> => {
  await deleteCachedValues([cacheKey, indexKey]);
};

const pruneGraphCacheEntries = async (
  currentTime: number,
  maximumEntries: number,
): Promise<void> => {
  const graphRecords = await getGraphCacheRecords();
  graphRecords.sort(
    (left, right) => right.lastAccessedAt - left.lastAccessedAt,
  );

  await Promise.all(
    graphRecords.map(async (record, index) => {
      const isExpired =
        currentTime - record.lastAccessedAt > GRAPH_CACHE_MAX_AGE_MS;
      if (isExpired || index >= maximumEntries) {
        await deleteGraphCacheRecord(record.cacheKey, record.indexKey);
      }
    }),
  );
};

const evictOldestGraphCacheEntry = async (
  preservedCacheKey: string,
): Promise<void> => {
  const graphRecords = await getGraphCacheRecords();
  const oldestRecord = graphRecords
    .filter((record) => record.cacheKey !== preservedCacheKey)
    .sort((left, right) => left.lastAccessedAt - right.lastAccessedAt)[0];
  if (oldestRecord !== undefined) {
    await deleteGraphCacheRecord(oldestRecord.cacheKey, oldestRecord.indexKey);
  }
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

  await pruneGraphCacheEntries(currentTime, GRAPH_CACHE_MAX_ENTRIES);

  const cachedKeys = await getAllCachedKeys();
  const stringCachedKeys = new Set(
    cachedKeys.filter((key): key is string => typeof key === "string"),
  );
  await Promise.all(
    [...stringCachedKeys].map(async (key) => {
      if (key.startsWith("graphData_") || key.includes("cache_id=")) {
        await deleteCachedValue(key);
        return;
      }

      if (
        key.startsWith("graph:") &&
        !stringCachedKeys.has(getGraphCacheIndexKey(key))
      ) {
        await deleteCachedValue(key);
        return;
      }

      if (key.startsWith(GRAPH_CACHE_INDEX_PREFIX)) {
        const cacheKey = key.slice(GRAPH_CACHE_INDEX_PREFIX.length);
        if (!stringCachedKeys.has(cacheKey)) {
          await deleteCachedValue(key);
        }
      }
    }),
  );
  await setCachedValue(GRAPH_CACHE_CLEANUP_KEY, currentTime);
};
