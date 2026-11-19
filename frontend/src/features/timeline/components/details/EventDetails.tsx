import { Link } from "@tanstack/react-router";
import {
  findPreviousSpec,
  resolveSpecSelection,
  useTableSpecs,
} from "../../../specs/tableSpecs";
import PanelIssueNotice from "../../../../components/PanelIssueNotice";
import { buildEventDetails } from "../../lib/details/buildEventDetails";
import type { TimelineData, TimelineRow } from "../../lib/timelineRow";
import BeforeAfterSection from "./BeforeAfterSection";
import DetailValueRow from "./DetailValueRow";
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
  const { specsMetadata } = useTableSpecs();
  const schemaChange = details.schemaChange;
  const schema =
    schemaChange === null
      ? null
      : resolveSpecSelection(specsMetadata, {
          kind: "schema",
          id: schemaChange.after,
        });
  const noticeTexts = (kind: "error" | "warning"): string[] =>
    details.notices
      .filter((notice) => notice.kind === kind)
      .map((notice) => notice.text);
  const hasThisChange =
    details.thisChange.counts.length > 0 || details.thisChange.rest.length > 0;

  return (
    <div className="flex flex-col gap-4">
      <PanelIssueNotice type="error">{noticeTexts("error")}</PanelIssueNotice>
      <PanelIssueNotice type="warning">
        {noticeTexts("warning")}
      </PanelIssueNotice>
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
          className="block font-mono text-xs break-all text-accent-text hover:underline"
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
      {schemaChange !== null &&
        schema?.type === "schema" &&
        String(
          findPreviousSpec(specsMetadata?.schemas ?? [], schema.data)?.[
            "schema-id"
          ],
        ) === String(schemaChange.before) && (
          <Link
            to="."
            search={(previous: Record<string, unknown>) => ({
              ...previous,
              spec_kind: "schema",
              spec_id: String(schemaChange.after),
              spec_view: "diff",
            })}
            state={(previous) => ({ ...previous, specsOverlay: true })}
            className="block font-mono text-sm break-all text-accent-text hover:underline"
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
      {details.rawDiff.length > 0 && <RawDiffSection diffs={details.rawDiff} />}
    </div>
  );
};

export default EventDetails;
