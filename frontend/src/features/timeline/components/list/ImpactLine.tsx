import { cn } from "../../../../shared/lib/cn";
import type { ImpactSegment } from "../../lib/impactSegment";
import { CHIP_BASE_CLASS } from "../Chip";
import type { BranchAccent } from "../eventColor";
import ImpactCount from "./ImpactCount";
import ImpactSize from "./ImpactSize";

const TAG_CHIP_CLASS = "bg-slate-500/15 text-slate-300";

interface ImpactLineProps {
  segments: ImpactSegment[];
  branchAccents: ReadonlyMap<string, BranchAccent>;
}

const isBalancedRowChurn = (segment: ImpactSegment): boolean =>
  segment.kind === "count" &&
  segment.unit === "rows" &&
  segment.added > 0 &&
  segment.added === segment.removed;

const segmentKey = (segment: ImpactSegment): string => {
  if (segment.kind === "text") {
    return segment.text;
  }
  if (segment.kind === "size") {
    return "size";
  }
  if (segment.kind === "ref") {
    return `${segment.name} ${segment.action}`;
  }
  return segment.unit;
};

const ImpactLine = ({ segments, branchAccents }: ImpactLineProps) => {
  const shownSegments = segments.filter(
    (segment) => !isBalancedRowChurn(segment),
  );

  return (
    <>
      {shownSegments.map((segment) => (
        <span
          key={segmentKey(segment)}
          className="not-first:before:mx-1.5 not-first:before:content-['·']"
        >
          {segment.kind === "text" ? (
            segment.text
          ) : segment.kind === "ref" ? (
            <>
              {segment.refType}{" "}
              <span
                className={cn(
                  CHIP_BASE_CLASS,
                  branchAccents.get(segment.name)?.chip ?? TAG_CHIP_CLASS,
                )}
              >
                {segment.name}
              </span>{" "}
              {segment.action}
            </>
          ) : segment.kind === "size" ? (
            <ImpactSize netBytes={segment.netBytes} />
          ) : (
            <ImpactCount
              added={segment.added}
              removed={segment.removed}
              unit={segment.unit}
            />
          )}
        </span>
      ))}
    </>
  );
};

export default ImpactLine;
