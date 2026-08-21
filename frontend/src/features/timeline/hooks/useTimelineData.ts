import JSONbig from "json-bigint";
import { useTableSpecs } from "../../../context/tableSpecs";
import { graphDataPayloadSchema } from "../api/graphDataPayloadSchema";
import { buildTimeline } from "../lib/buildTimeline";
import type { TimelineData } from "../lib/timelineRow";

export const useTimelineData = (): TimelineData => {
  const { rawData } = useTableSpecs();
  if (rawData === null) {
    throw new Error("useTimelineData rendered before graph data loaded");
  }
  const payload = graphDataPayloadSchema.parse(
    JSONbig({ storeAsString: true }).parse(rawData),
  );
  return buildTimeline(payload.nodes, payload.metadata);
};
