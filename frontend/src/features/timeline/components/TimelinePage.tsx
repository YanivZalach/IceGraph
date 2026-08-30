import { useSearch } from "@tanstack/react-router";
import { useTimelineData } from "../hooks/useTimelineData";
import TimelineContent from "./TimelineContent";

const TimelinePage = () => {
  const { table } = useSearch({ from: "/table" });
  const timeline = useTimelineData();

  return <TimelineContent key={table} timeline={timeline} table={table} />;
};

export default TimelinePage;
