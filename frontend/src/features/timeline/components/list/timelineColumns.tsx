import { createColumnHelper, tableFeatures } from "@tanstack/table-core";
import type { DisplayColumnDef } from "@tanstack/table-core";
import type { TimelineRow } from "../../lib/timelineRow";
import { displayBranchNameFor } from "../../lib/displayBranchName";
import type { BranchAccent } from "../eventColor";
import CommitCell from "./CommitCell";
import EventRail from "./EventRail";
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
  const branchName = displayBranchNameFor(row);
  return branchName === null ? undefined : branchAccents.get(branchName)?.node;
};

const railMarker = (row: TimelineRow): "node" | "tick" | "none" => {
  if (row.kind === "boundary") {
    return "none";
  }
  const isSnapshotWrite =
    row.kind === "published-write" || row.kind === "draft-write";
  return isSnapshotWrite ? "node" : "tick";
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
    header: "Commit",
    cell: ({ row }) => (
      <CommitCell row={row.original} branchAccents={branchAccents} />
    ),
  }),
  columnHelper.display({
    id: "changes",
    header: "Changes",
    cell: ({ row }) =>
      row.original.kind === "boundary" ? null : (
        <ImpactLine
          segments={row.original.impact}
          branchAccents={branchAccents}
        />
      ),
  }),
  columnHelper.display({
    id: "snapshot",
    header: "Snapshot",
    cell: ({ row }) => row.original.shortId,
  }),
  columnHelper.display({
    id: "time",
    header: "Time",
    cell: ({ row }) => (
      <TimeCell timestampMs={row.original.timestampMs} nowMs={nowMs} />
    ),
  }),
];
