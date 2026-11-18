import { createColumnHelper, tableFeatures } from "@tanstack/react-table";
import type { DisplayColumnDef } from "@tanstack/react-table";
import type { TimelineRow } from "../../lib/timelineRow";
import type { BranchAccent } from "../eventColor";
import CommitCell from "./CommitCell";
import EventRail, { type RailMarker } from "./EventRail";
import { impactPlainText } from "../../lib/impactSegment";
import ImpactLine from "./ImpactLine";
import TimeCell from "./TimeCell";

export const timelineTableFeatures = tableFeatures({});

const columnHelper = createColumnHelper<
  typeof timelineTableFeatures,
  TimelineRow
>();

const railNodeClassFor = (
  row: TimelineRow,
  branchAccents: ReadonlyMap<string, BranchAccent>,
): string | undefined => {
  return row.kind === "gap" || row.branchName === null
    ? undefined
    : branchAccents.get(row.branchName)?.node;
};

const railMarker = (row: TimelineRow): RailMarker => {
  if (row.kind === "boundary") {
    return "none";
  }
  if (row.kind === "gap") {
    return "gap";
  }
  if (row.kind === "draft-write") {
    return "draft";
  }
  return row.kind === "published-write" ? "node" : "tick";
};

export const buildTimelineColumns = (
  nowMs: number,
  branchAccents: ReadonlyMap<string, BranchAccent>,
): DisplayColumnDef<typeof timelineTableFeatures, TimelineRow>[] => [
  columnHelper.display({
    id: "rail",
    header: "",
    cell: ({ row }) => (
      <EventRail
        marker={railMarker(row.original)}
        nodeClassName={railNodeClassFor(row.original, branchAccents)}
      />
    ),
  }),
  columnHelper.display({
    id: "commit",
    header: "Event",
    cell: ({ row }) => (
      <CommitCell row={row.original} branchAccents={branchAccents} />
    ),
  }),
  columnHelper.display({
    id: "changes",
    header: "Changes",
    cell: ({ row }) =>
      row.original.kind === "boundary" || row.original.kind === "gap" ? null : (
        <span title={impactPlainText(row.original.impact)}>
          <ImpactLine
            segments={row.original.impact}
            branchAccents={branchAccents}
          />
        </span>
      ),
  }),
  columnHelper.display({
    id: "snapshot",
    header: "Snapshot",
    cell: ({ row }) =>
      row.original.kind === "gap" ? "" : row.original.shortId,
  }),
  columnHelper.display({
    id: "time",
    header: "Time",
    cell: ({ row }) => (
      <TimeCell timestampMs={row.original.timestampMs} nowMs={nowMs} />
    ),
  }),
];
