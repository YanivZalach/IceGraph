import type { TableMetadata } from "../../table/api/metadataSchemas";
import type { SpecSelection } from "../../specs/tableSpecs";
import { parseIcebergSchema } from "../../schema/schemaModel";
import { integerText, schemaColumnNames } from "../metadataPresentation";
import MetadataSchema from "./MetadataSchema";
import MetadataPartitioning from "./MetadataPartitioning";
import MetadataSortOrder from "./MetadataSortOrder";
import MetadataTechnicalDetails from "./MetadataTechnicalDetails";
import MetadataJson from "./MetadataJson";
import {
  UI_EXPANDABLE_BODY_CLASS,
  UI_EXPANDABLE_SUMMARY_CLASS,
} from "../../../uiTypography";

interface MetadataDetailsProps {
  metadata: TableMetadata;
  onOpenSpec: (selection: SpecSelection) => void;
}

const MetadataDetails = ({ metadata, onOpenSpec }: MetadataDetailsProps) => {
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
    <>
      <MetadataSchema
        key={metadata.metadata_file_path}
        schema={schema}
        schemas={metadata.schemas ?? []}
        onOpenSpec={onOpenSpec}
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
          onOpenSpec={onOpenSpec}
        />
        <MetadataSortOrder
          sortOrder={sortOrder}
          columnNames={names}
          onOpenSpec={onOpenSpec}
        />
      </section>
      <MetadataTechnicalDetails metadata={metadata} />
      <details className="overflow-hidden rounded-xl border border-edge bg-surface">
        <summary className={UI_EXPANDABLE_SUMMARY_CLASS}>
          Advanced: metadata JSON
        </summary>
        <div className={`${UI_EXPANDABLE_BODY_CLASS} space-y-3 px-5 py-4`}>
          <p className="text-xs leading-relaxed text-slate-400">
            Reduced metadata: the backend omits <code>metadata-log</code>,{" "}
            <code>snapshot-log</code>, <code>snapshots</code>, and{" "}
            <code>statistics</code> due to size. This is the returned metadata,
            not the complete file.
          </p>
          <MetadataJson
            text={JSON.stringify(metadata, null, 2)}
            label="metadata JSON"
          />
        </div>
      </details>
    </>
  );
};
export default MetadataDetails;
