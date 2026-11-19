import { cn } from "../../../../shared/lib/cn";
import type { LaneLine, RowLanes } from "../../lib/buildBranchLanes";
import type { BranchAccent } from "../eventColor";
import { laneX, railWidthFor } from "./columnClasses";
import type { RailMarker } from "./EventRail";

interface BranchGraphRailProps {
  lanes: RowLanes;
  laneCount: number;
  marker: RailMarker;
  nodeBranchName: string | null;
  branchAccents: ReadonlyMap<string, BranchAccent>;
}

const EDGE_BOX_HEIGHT = 10;
const LINE_WIDTH = 1.5;
const HOVER_WIDTH = 8;
const HOVER_CLASS = "stroke-transparent";

const BranchGraphRail = ({
  lanes,
  laneCount,
  marker,
  nodeBranchName,
  branchAccents,
}: BranchGraphRailProps) => {
  const width = railWidthFor(laneCount);
  const strokeOf = (branchName: string | null): string =>
    (branchName === null ? undefined : branchAccents.get(branchName)?.stroke) ??
    "stroke-edge";
  const lineClass = (line: LaneLine): string =>
    lanes.isGap ? "stroke-amber-500/60" : strokeOf(line.branchName);
  const renderLines = (lines: LaneLine[], y1: string, y2: string) =>
    lines.map((line) => (
      <g key={`${y1}-${line.lane.toString()}`}>
        <title>{line.branchName}</title>
        <line
          x1={laneX(line.lane)}
          x2={laneX(line.lane)}
          y1={y1}
          y2={y2}
          strokeWidth={LINE_WIDTH}
          strokeDasharray={lanes.isGap ? "3 3" : undefined}
          className={lineClass(line)}
        />
        <line
          x1={laneX(line.lane)}
          x2={laneX(line.lane)}
          y1={y1}
          y2={y2}
          strokeWidth={HOVER_WIDTH}
          className={HOVER_CLASS}
        />
      </g>
    ));
  const nodeX = laneX(lanes.nodeLane);
  let nodeLaneName = "main";
  for (const line of [...lanes.through, ...lanes.starting, ...lanes.ending]) {
    if (line.lane === lanes.nodeLane) {
      nodeLaneName = line.branchName;
      break;
    }
  }
  const nodeTitle = marker === "draft" ? "unpublished" : nodeLaneName;
  const nodeStroke =
    (nodeBranchName === null
      ? undefined
      : branchAccents.get(nodeBranchName)?.stroke) ?? "stroke-accent";

  return (
    <>
      <span className="block" style={{ width }} />
      <svg
        className="absolute inset-0 h-full overflow-visible"
        width={width}
        aria-hidden="true"
      >
        {renderLines(lanes.through, "0", "100%")}
        {renderLines(lanes.starting, "0", "50%")}
        {renderLines(lanes.ending, "50%", "100%")}
        <svg
          y="50%"
          height="50%"
          width={width}
          viewBox={`0 0 ${width.toString()} ${EDGE_BOX_HEIGHT.toString()}`}
          preserveAspectRatio="none"
          overflow="visible"
        >
          {lanes.edges.map((edge) => {
            const fromX = laneX(edge.fromLane);
            const toX = laneX(edge.toLane);
            const middleY = EDGE_BOX_HEIGHT / 2;
            const path = `M ${fromX.toString()} ${EDGE_BOX_HEIGHT.toString()} C ${fromX.toString()} ${middleY.toString()}, ${toX.toString()} ${middleY.toString()}, ${toX.toString()} 0`;
            return (
              <g
                key={`${edge.kind}-${edge.fromLane.toString()}-${edge.toLane.toString()}`}
              >
                {edge.branchName !== null && <title>{edge.branchName}</title>}
                <path
                  d={path}
                  fill="none"
                  strokeWidth={LINE_WIDTH}
                  vectorEffect="non-scaling-stroke"
                  className={strokeOf(edge.branchName)}
                />
                <path
                  d={path}
                  fill="none"
                  strokeWidth={HOVER_WIDTH}
                  vectorEffect="non-scaling-stroke"
                  className={HOVER_CLASS}
                />
              </g>
            );
          })}
        </svg>
        <g>
          <title>{nodeTitle}</title>
          {(marker === "node" || marker === "draft") && (
            <circle
              cx={nodeX}
              cy="50%"
              r={4}
              strokeWidth={2}
              className={cn(
                marker === "draft" ? "fill-white" : "fill-surface",
                nodeStroke,
              )}
            />
          )}
          {marker === "tick" && (
            <line
              x1={nodeX - 5}
              x2={nodeX + 5}
              y1="50%"
              y2="50%"
              strokeWidth={2}
              className="stroke-slate-500"
            />
          )}
        </g>
      </svg>
    </>
  );
};

export default BranchGraphRail;
