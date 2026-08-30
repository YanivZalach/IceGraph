import { useTableGraphData } from "../../table/tableGraphData";
import { graphDataPayloadSchema } from "../api/graphDataPayloadSchema";
import { buildTimeline } from "../lib/buildTimeline";
import type { TimelineData } from "../lib/timelineRow";

export const useTimelineData = (): TimelineData => {
  const graphData = useTableGraphData();
  const payload = graphDataPayloadSchema.parse(graphData);
  return buildTimeline(payload.nodes, payload.metadata);
};
