import type { ImpactSegment } from "../lib/impactSegment";
import ImpactCount from "./ImpactCount";

interface ImpactLineProps {
  segments: ImpactSegment[];
}

const isRewriteRowChurn = (segment: ImpactSegment): boolean =>
  segment.kind === "count" &&
  segment.unit === "rows" &&
  segment.added > 0 &&
  segment.removed > 0;

const segmentKey = (segment: ImpactSegment): string =>
  segment.kind === "text" ? segment.text : segment.unit;

const ImpactLine = ({ segments }: ImpactLineProps) => {
  const shownSegments = segments.filter(
    (segment) => !isRewriteRowChurn(segment),
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
