import { PanelDetailRow } from "../../../../components/PanelContent";
import { cn } from "../../../../shared/lib/cn";
import { buildEventDetails } from "../../lib/details/buildEventDetails";
import type { TimelineData, TimelineRow } from "../../lib/timelineRow";
import BeforeAfterSection from "./BeforeAfterSection";
import ChangeCountsSection from "./ChangeCountsSection";
import CollapsibleTableSection from "./CollapsibleTableSection";
import MetadataFileSection from "./MetadataFileSection";
import RawDiffSection from "./RawDiffSection";
import ViewInGraphButtons from "./ViewInGraphButtons";

interface EventDetailsProps {
  timeline: TimelineData;
  row: TimelineRow;
}

const EventDetails = ({ timeline, row }: EventDetailsProps) => {
  const details = buildEventDetails(timeline, row);
  const hasThisChange =
    details.thisChange.counts.length > 0 || details.thisChange.rest.length > 0;

  return (
    <div className="flex flex-col gap-4">
      {details.notices.map((notice) => (
        <p
          key={notice.text}
          className={cn(
            "rounded border px-3 py-2 text-xs",
            notice.kind === "error"
              ? "border-red-500/40 bg-red-500/10 text-red-300"
              : "border-amber-500/40 bg-amber-500/10 text-amber-300",
          )}
        >
          {notice.text}
        </p>
      ))}
      <ViewInGraphButtons
        snapshotFilePath={details.snapshotFilePath}
        metadataFilePath={details.metadataFile.path}
      />
      {details.actionLink !== null && (
        <a
          href={details.actionLink}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="Open the Spark job page in a new tab"
          className="block font-mono text-xs break-all text-accent hover:text-accent-dark"
        >
          {details.actionLink} ↗
        </a>
      )}
      {details.topRows.map((rowData) => (
        <PanelDetailRow
          key={rowData.label}
          label={rowData.label}
          value={rowData.value}
        />
      ))}
      {hasThisChange && (
        <ChangeCountsSection
          title="This change"
          counts={details.thisChange.counts}
          rows={details.thisChange.rest}
        />
      )}
      {details.tableState.length > 0 && (
        <BeforeAfterSection title="Table state" rows={details.tableState} />
      )}
      {details.engine.length > 0 && (
        <CollapsibleTableSection title="Engine" rows={details.engine} />
      )}
      <MetadataFileSection data={details.metadataFile} />
      {details.refs.length > 0 && (
        <CollapsibleTableSection title="Refs" rows={details.refs} />
      )}
      {details.properties.length > 0 && (
        <CollapsibleTableSection title="Properties" rows={details.properties} />
      )}
      {details.rawDiff.length > 0 && <RawDiffSection diffs={details.rawDiff} />}
    </div>
  );
};

export default EventDetails;
