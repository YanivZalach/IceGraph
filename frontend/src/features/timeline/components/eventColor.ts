import type { TimelineRow } from "../lib/timelineRow";

/** The legend colors of the legacy Timeline (TimelinePage.jsx COLOR_*). */
const WRITE_COLOR = "#1964B9";
const METADATA_COLOR = "#6437D2";
const BRANCH_COLOR = "#0EA5E9";

export const eventColorFor = (row: TimelineRow): string => {
  if (row.kind === "draft-write") {
    return BRANCH_COLOR;
  }
  if (row.kind === "published-write") {
    return row.branchName === "main" || row.branchName === null
      ? WRITE_COLOR
      : BRANCH_COLOR;
  }
  return METADATA_COLOR;
};
