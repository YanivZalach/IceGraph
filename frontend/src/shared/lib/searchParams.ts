import { z } from "zod";

// TanStack's default parse/stringify JSON-parses each value, which silently
// rounds snapshot IDs above Number.MAX_SAFE_INTEGER. URLSearchParams also keeps
// emitted URLs byte-identical to the legacy pages', which the dup-tab cache key
// (window.location.href) depends on.
// See claude-plugin/skills/icegraph/SKILL.md "URL structure".
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
// from the URL. SKILL.md contracts that params round-trip untouched.
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

export const fileTreeSearchSchema = tableSearchSchema.extend({
  filetree_branch: z.string().optional(),
  filetree_grouping: z.enum(["flat", "tree"]).optional(),
  filetree_scope: z.enum(["commit", "snapshot"]).optional(),
  filetree_search: z.string().optional(),
  filetree_snapshot_id: z.string().optional(),
});

export const snapshotSelectionSearchSchema = z.looseObject({
  table: z.string().optional(),
});

export const docsSearchSchema = z.looseObject({
  section: z.string().optional(),
});
