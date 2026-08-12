import { z } from "zod";

// Custom parse/stringify instead of TanStack's defaults, which JSON-parse each
// value: snapshot IDs exceed Number.MAX_SAFE_INTEGER and would silently lose
// precision (see claude-plugin/skills/icegraph/SKILL.md "URL structure").
// URLSearchParams is the same serializer the legacy pages use to build URLs,
// keeping router-emitted URLs byte-identical for the IndexedDB dup-cache key.
export const parseSearch = (searchString: string): Record<string, string> =>
  Object.fromEntries(
    new URLSearchParams(
      searchString.startsWith("?") ? searchString.slice(1) : searchString,
    ),
  );

export const stringifySearch = (search: Record<string, unknown>): string => {
  const urlSearchParams = new URLSearchParams();
  for (const [key, value] of Object.entries(search)) {
    if (typeof value === "string") {
      urlSearchParams.set(key, value);
    } else if (typeof value === "number" || typeof value === "boolean") {
      urlSearchParams.set(key, String(value));
    }
  }
  const serialized = urlSearchParams.toString();
  return serialized ? `?${serialized}` : "";
};

// z.looseObject is load-bearing: a plain z.object strips unknown params from
// carry-forward navigations (search: (prev) => prev), silently dropping them
// from the URL — see SKILL.md's contract that params round-trip untouched.
export const tableSearchSchema = z.looseObject({
  table: z.string().optional(),
  start_snapshot_id: z.string().optional(),
  end_snapshot_id: z.string().optional(),
  dup: z.string().optional(),
  cache_id: z.string().optional(),
});

export const graphSearchSchema = tableSearchSchema.extend({
  select_node_id: z.string().optional(),
});

export const snapshotSelectionSearchSchema = z.looseObject({
  table: z.string().optional(),
});
