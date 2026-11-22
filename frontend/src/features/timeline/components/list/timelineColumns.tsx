import { createColumnHelper, tableFeatures } from "@tanstack/react-table";
import type { DisplayColumnDef } from "@tanstack/react-table";
import type { BranchGraph } from "../../lib/buildBranchLanes";
import type { TimelineRow } from "../../lib/timelineRow";
import type { BranchAccent } from "../eventColor";
import BranchGraphRail from "./BranchGraphRail";
import CommitCell from "./CommitCell";
import { railWidthFor } from "./columnClasses";
import EventRail, { type RailMarker } from "./EventRail";
import { impactPlainText } from "../../lib/impactSegment";
import ImpactLine from "./ImpactLine";
import TimeCell from "./TimeCell";

export const timelineTableFeatures = tableFeatures({});

const columnHelper = createColumnHelper<
  typeof timelineTableFeatures,
  TimelineRow
>();

const nodeBranchNameOf = (row: TimelineRow): string | null =>
  row.kind === "gap" ? null : row.branchName;

const railMarker = (row: TimelineRow): RailMarker => {
  if (row.kind === "unknown-change") {
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
  branchGraph: BranchGraph,
  isFlatRail: boolean,
): DisplayColumnDef<typeof timelineTableFeatures, TimelineRow>[] => [
  columnHelper.display({
    id: "rail",
    header: "",
    cell: ({ row }) => {
      const marker = railMarker(row.original);
      const nodeBranchName =
        marker === "draft" ? "main" : nodeBranchNameOf(row.original);
      const lanes = branchGraph.lanesByFilePath.get(row.original.filePath);
      if (isFlatRail || lanes === undefined) {
        return (
          <>
            <span
              className="block"
              style={{ width: railWidthFor(branchGraph.laneCount) }}
            />
            <span className="absolute inset-y-0 left-0 w-10">
              <EventRail
                marker={marker}
                nodeClassName={
                  nodeBranchName === null
                    ? undefined
                    : branchAccents.get(nodeBranchName)?.node
                }
              />
            </span>
          </>
        );
      }
      return (
        <BranchGraphRail
          lanes={lanes}
          laneCount={branchGraph.laneCount}
          marker={marker}
          nodeBranchName={nodeBranchName}
          branchAccents={branchAccents}
        />
      );
    },
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
      row.original.kind === "gap" ? null : (
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
