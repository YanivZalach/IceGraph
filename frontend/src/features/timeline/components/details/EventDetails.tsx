import { PanelDetailRow } from "../../../../components/PanelContent";
import type { MetadataFileNode, SnapshotNode } from "../../api/nodeSchemas";
import { diffMetadataFiles } from "../../lib/details/diffMetadataFiles";
import { buildEventDetails } from "../../lib/details/buildEventDetails";
import type { TimelineRow } from "../../lib/timelineRow";
import BeforeAfterSection from "./BeforeAfterSection";
import ChangeCountsSection from "./ChangeCountsSection";
import CollapsibleTableSection from "./CollapsibleTableSection";
import MetadataFileSection from "./MetadataFileSection";
import RawDiffSection from "./RawDiffSection";
import ViewInGraphButtons from "./ViewInGraphButtons";

interface EventDetailsProps {
  row: TimelineRow;
  file: MetadataFileNode;
  previousFile: MetadataFileNode | undefined;
  snapshot: SnapshotNode | undefined;
  parentSnapshot: SnapshotNode | undefined;
  repointTarget: SnapshotNode | undefined;
}

const EventDetails = ({
  row,
  file,
  previousFile,
  snapshot,
  parentSnapshot,
  repointTarget,
}: EventDetailsProps) => {
  const details = buildEventDetails(
    row,
    file,
    snapshot,
    repointTarget,
    parentSnapshot?.summary ?? null,
  );
  const hasThisChange =
    details.thisChange.counts.length > 0 || details.thisChange.rest.length > 0;
  const actionLink = snapshot?.action_link ?? "";

  return (
    <div className="flex flex-col gap-4">
      <ViewInGraphButtons
        snapshotFilePath={snapshot?.file_path ?? null}
        metadataFilePath={file.file_path}
      />
      {actionLink !== "" && (
        <a
          href={actionLink}
          target="_blank"
          rel="noopener noreferrer"
          aria-label="Open the Spark job page in a new tab"
          className="block font-mono text-xs break-all text-accent hover:text-accent-dark"
        >
          {actionLink} ↗
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
      {previousFile !== undefined && (
        <RawDiffSection diffs={diffMetadataFiles(previousFile, file)} />
      )}
    </div>
  );
};

export default EventDetails;
