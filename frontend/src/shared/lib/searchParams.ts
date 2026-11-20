import { z } from "zod";

// TanStack's default parse/stringify JSON-parses each value, which silently
// rounds snapshot IDs above Number.MAX_SAFE_INTEGER. URLSearchParams also keeps
// emitted URLs byte-identical to the documented URL structure.
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
const specSearchShape = {
  specs: z.literal("open").optional().catch(undefined),
  spec_kind: z
    .enum(["schema", "partition", "order"])
    .optional()
    .catch(undefined),
  spec_id: z.string().optional(),
  spec_view: z.literal("diff").optional().catch(undefined),
};

export const tableSearchSchema = z.looseObject({
  table: z.string().optional(),
  start_snapshot_id: z.string().optional(),
  end_snapshot_id: z.string().optional(),
  ...specSearchShape,
});

const graphSelectionShape = {
  select_node_id: z.string().optional(),
  lineage_node_id: z.string().optional(),
  lineage_partition: z.string().optional(),
  graph_mode: z.literal("lineage").optional().catch(undefined),
  graph_search: z.string().optional(),
};

export const graphSearchSchema = tableSearchSchema.extend(graphSelectionShape);

const timelineSearchShape = {
  timeline_event: z.string().optional(),
  timeline_kind: z
    .enum(["all", "writes", "metadata"])
    .optional()
    .catch(undefined),
  timeline_branch: z.string().optional(),
};

export const timelineSearchSchema =
  tableSearchSchema.extend(timelineSearchShape);

export const TIMELINE_SEARCH_KEYS = Object.keys(timelineSearchShape);

export const fileTreeSearchSchema = tableSearchSchema.extend({
  filetree_branch: z.string().optional(),
  filetree_grouping: z.enum(["flat", "tree"]).optional(),
  filetree_scope: z.enum(["commit", "snapshot"]).optional(),
  filetree_search: z.string().optional(),
  filetree_snapshot_id: z.string().optional(),
});

export const snapshotSelectionSearchSchema = z.looseObject({
  table: z.string().optional(),
  partition_snapshot: z.string().optional(),
  ...specSearchShape,
});

export const docsSearchSchema = z.looseObject({
  section: z.string().optional(),
});

// Partition filter values are carried as "p.<partition field name>" params.
export const partitionSearchSchema = z.looseObject({
  table: z.string().optional(),
  snapshot: z.string().optional(),
  ...specSearchShape,
});

export const partitionGraphSearchSchema = partitionSearchSchema.extend({
  ...graphSelectionShape,
});

export const partitionFilesSearchSchema = partitionSearchSchema.extend({
  filetree_group_by: z.string().optional(),
  filetree_search: z.string().optional(),
});
