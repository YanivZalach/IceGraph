import type { ImpactSegment } from "../../lib/impactSegment";
import ImpactCount from "./ImpactCount";
import ImpactSize from "./ImpactSize";

interface ImpactLineProps {
  segments: ImpactSegment[];
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
  return segment.unit;
};

const ImpactLine = ({ segments }: ImpactLineProps) => {
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
