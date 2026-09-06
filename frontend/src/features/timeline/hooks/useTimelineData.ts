import { useQueryClient, useSuspenseQuery } from "@tanstack/react-query";
import { useSearch } from "@tanstack/react-router";
import { graphQueryOptions } from "../../table/api/graphQueries";
import { graphDataPayloadSchema } from "../api/graphDataPayloadSchema";
import { buildTimeline } from "../lib/buildTimeline";
import type { TimelineData } from "../lib/timelineRow";

/**
 * The request parameters must be built the same way `TableSpecsProvider` builds them, or the
 * differing query key starts a second collection job.
 */
export const useTimelineData = (): TimelineData => {
  const search = useSearch({ from: "/table" });
  const queryClient = useQueryClient();
  const graphQuery = useSuspenseQuery(
    graphQueryOptions(
      {
        tableName: search.table ?? "",
        startSnapshotId: search.start_snapshot_id ?? "",
        endSnapshotId: search.end_snapshot_id ?? "",
      },
      queryClient,
    ),
  );
  const payload = graphDataPayloadSchema.parse(graphQuery.data);
  return buildTimeline(payload.nodes, payload.metadata);
};
