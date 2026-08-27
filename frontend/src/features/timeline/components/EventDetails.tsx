import { Fragment } from "react";
import { PanelDetailRow } from "../../../components/PanelContent";
import type { MetadataFileNode, SnapshotNode } from "../api/nodeSchemas";
import { buildEventDetailSections } from "../lib/buildEventDetailSections";
import type { TimelineRow } from "../lib/timelineRow";
import ViewInGraphButtons from "./ViewInGraphButtons";

interface EventDetailsProps {
  row: TimelineRow;
  file: MetadataFileNode;
  snapshot: SnapshotNode | undefined;
  repointTarget: SnapshotNode | undefined;
}

const SECTION_TITLE_CLASS =
  "mt-2 mb-2 text-sm font-bold tracking-widest text-accent uppercase";

const EventDetails = ({
  row,
  file,
  snapshot,
  repointTarget,
}: EventDetailsProps) => {
  const sections = buildEventDetailSections(row, file, snapshot, repointTarget);
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
          className="block font-mono text-xs break-all text-accent hover:text-accent-dark"
        >
          {actionLink}
        </a>
      )}
      {sections.map((section) => (
        <Fragment key={section.title}>
          {section.title !== "" && (
            <div className={SECTION_TITLE_CLASS}>{section.title}</div>
          )}
          {section.rows.map((rowData) => (
            <PanelDetailRow
              key={rowData.label}
              label={rowData.label}
              value={rowData.value}
            />
          ))}
        </Fragment>
      ))}
    </div>
  );
};

export default EventDetails;
