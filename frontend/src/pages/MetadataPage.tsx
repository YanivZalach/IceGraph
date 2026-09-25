import { useSearch } from "@tanstack/react-router";
import { useTableSpecs } from "../features/specs/tableSpecs";
import { parseIcebergSchema } from "../features/schema/schemaModel";
import {
  integerText,
  schemaColumnNames,
} from "../features/metadata/metadataPresentation";
import { useMetadataKeyboardScroll } from "../features/metadata/useMetadataKeyboardScroll";
import MetadataVersion from "../features/metadata/components/MetadataVersion";
import MetadataSummary from "../features/metadata/components/MetadataSummary";
import MetadataSchema from "../features/metadata/components/MetadataSchema";
import MetadataPartitioning from "../features/metadata/components/MetadataPartitioning";
import MetadataSortOrder from "../features/metadata/components/MetadataSortOrder";
import MetadataTechnicalDetails from "../features/metadata/components/MetadataTechnicalDetails";
import MetadataJson from "../features/metadata/components/MetadataJson";
import {
  UI_EXPANDABLE_BODY_CLASS,
  UI_EXPANDABLE_SUMMARY_CLASS,
} from "../uiTypography";
import { FileType } from "../graphConstants";

const MetadataPage = () => {
  const search = useSearch({ from: "/table/metadata" });
  const { graphQuery, openSpec, detailsOpen, issuesOpen } = useTableSpecs();
  const data = graphQuery.data;
  useMetadataKeyboardScroll(detailsOpen || issuesOpen);
  if (!data) return null;
  const { metadata, nodes } = data;
  const path = metadata.metadata_file_path;
  const snapshotId = integerText(metadata["current-snapshot-id"]);
  const snapshot = nodes.find(
    (node) =>
      node.type === FileType.SNAPSHOT &&
      integerText(node.snapshot_id) === snapshotId,
  );
  const schema = metadata.schemas?.find(
    (item) =>
      integerText(item["schema-id"]) ===
      integerText(metadata["current-schema-id"]),
  );
  const partitionSpec = metadata["partition-specs"]?.find(
    (item) =>
      integerText(item["spec-id"]) === integerText(metadata["default-spec-id"]),
  );
  const sortOrder = metadata["sort-orders"]?.find(
    (item) =>
      integerText(item["order-id"]) ===
      integerText(metadata["default-sort-order-id"]),
  );
  const names = schemaColumnNames(parseIcebergSchema(schema));
  return (
    <div className="min-w-0 flex-1 bg-canvas">
      <main className="mx-auto flex max-w-5xl flex-col gap-7 px-5 py-8 md:px-8">
        <header>
          <p className="text-xs tracking-widest text-slate-400 uppercase">
            Table metadata
          </p>
          <h1 className="mt-1 break-all text-2xl font-semibold text-ink">
            {metadata["table-name"] ?? search.table}
          </h1>
        </header>
        <MetadataVersion path={path} updatedAt={metadata["last-updated-ms"]} />
        <MetadataSummary snapshotId={snapshotId} snapshot={snapshot} />
        <MetadataSchema
          key={path}
          schema={schema}
          schemas={metadata.schemas ?? []}
          onOpenSpec={openSpec}
        />
        <section
          aria-labelledby="metadata-organization-title"
          className="space-y-3"
        >
          <h2
            id="metadata-organization-title"
            className="text-base font-semibold text-ink"
          >
            Data organization
          </h2>
          <MetadataPartitioning
            partitionSpec={partitionSpec}
            columnNames={names}
            onOpenSpec={openSpec}
          />
          <MetadataSortOrder
            sortOrder={sortOrder}
            columnNames={names}
            onOpenSpec={openSpec}
          />
        </section>
        <MetadataTechnicalDetails metadata={metadata} snapshot={snapshot} />
        <details className="overflow-hidden rounded-xl border border-edge bg-surface">
          <summary className={UI_EXPANDABLE_SUMMARY_CLASS}>
            Advanced: metadata JSON
          </summary>
          <div className={`${UI_EXPANDABLE_BODY_CLASS} space-y-3 px-5 py-4`}>
            <p className="text-xs leading-relaxed text-slate-400">
              Reduced metadata: the backend omits <code>metadata-log</code>,{" "}
              <code>snapshot-log</code>, <code>snapshots</code>, and{" "}
              <code>statistics</code> due to size. This is the returned
              metadata, not the complete file.
            </p>
            <MetadataJson
              text={JSON.stringify(metadata, null, 2)}
              label="metadata JSON"
            />
          </div>
        </details>
      </main>
    </div>
  );
};
export default MetadataPage;
