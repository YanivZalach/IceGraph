import { Link } from "@tanstack/react-router";
import {
  isSchemaDirectlyAfter,
  schemaDiffSearch,
  SPECS_OVERLAY_STATE,
  useTableSpecs,
} from "../../../specs/tableSpecs";
import { buildEventDetails } from "../../lib/details/buildEventDetails";
import type { CommitRow, TimelineData } from "../../lib/timelineRow";
import BeforeAfterSection from "./BeforeAfterSection";
import DetailNoticeBox from "./DetailNoticeBox";
import DetailValueRow from "./DetailValueRow";
import ChangeCountsSection from "./ChangeCountsSection";
import CollapsibleTableSection from "./CollapsibleTableSection";
import MetadataFileSection from "./MetadataFileSection";
import RawDiffSection from "./RawDiffSection";
import ViewInGraphButtons from "./ViewInGraphButtons";

interface EventDetailsProps {
  timeline: TimelineData;
  row: CommitRow;
}

const EventDetails = ({ timeline, row }: EventDetailsProps) => {
  const details = buildEventDetails(timeline, row);
  const { specsMetadata } = useTableSpecs();
  const schemaChange = details.schemaChange;
  const canLinkSchemaDiff =
    schemaChange !== null &&
    isSchemaDirectlyAfter(
      specsMetadata,
      schemaChange.before,
      schemaChange.after,
    );
  const hasThisChange =
    details.thisChange.counts.length > 0 || details.thisChange.rest.length > 0;

  return (
    <div className="flex flex-col gap-4">
      {details.notices.map((notice) => (
        <DetailNoticeBox key={notice.text} kind={notice.kind}>
          {notice.text}
        </DetailNoticeBox>
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
        <DetailValueRow
          key={rowData.label}
          label={rowData.label}
          value={rowData.value}
        />
      ))}
      {schemaChange !== null && canLinkSchemaDiff && (
        <Link
          to="."
          search={(previous: Record<string, unknown>) => ({
            ...previous,
            ...schemaDiffSearch(schemaChange.after),
          })}
          state={(previous) => ({ ...previous, ...SPECS_OVERLAY_STATE })}
          className="block font-mono text-xs break-all text-accent hover:text-accent-dark"
        >
          View schema {schemaChange.after}
        </Link>
      )}
      {hasThisChange && (
        <ChangeCountsSection
          title="Change summary"
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
        <CollapsibleTableSection title="Refs" rows={details.refs} isVerbatim />
      )}
      {details.properties.length > 0 && (
        <CollapsibleTableSection
          title="Properties"
          rows={details.properties}
          isVerbatim
        />
      )}
      {details.rawDiff.length > 0 && (
        <RawDiffSection
          key={row.filePath}
          diffs={details.rawDiff}
          isOpenByDefault={details.isRawDiffOpenByDefault}
        />
      )}
    </div>
  );
};

export default EventDetails;
